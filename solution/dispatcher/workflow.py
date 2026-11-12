"""The coordinator workflow: one Temporal workflow that owns every lead.

It uses the rule book (scheduling.py) to decide and the activities (activities.py)
to act. Call results come in as the `call_result` signal, sent by the webhook
receiver.

Why one workflow for all leads: three rules are global. The tenant limit is shared
across campaigns (R2), a phone may not be called twice at once across campaigns (R3),
and DND blocks a phone everywhere (R8). One coordinator sees every lead, so those
checks are simple and cannot race.

Why it survives a crash: every decision, activity result, signal and timer is saved
in Temporal's history. A restarted worker replays that history through this same
code, which rebuilds exactly the same state, and then carries on.

Replay safety rules followed in this file:
  * time comes only from workflow.now(), never the machine clock
  * no network or file access (that happens in activities)
  * no decision depends on the iteration order of a Python set
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from .activities import PlaceCallInput
    from .clock import SimClock
    from .config import CampaignConfig
    from .provider import ACCEPTED, REJECTED, CallResult, PlaceCallOutcome
    from .scheduling import (
        LeadState,
        next_wake_time,
        pick_next,
        record_call_result,
        resolve_final_state,
    )

# How long one place_call activity may run before Temporal considers it lost (for
# example because our worker was killed) and runs it again with the same request_id.
PLACE_CALL_TIMEOUT = timedelta(seconds=30)
PLACE_CALL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=0,  # keep trying: an unknown outcome must be resolved, never dropped
)

# Simulated back-off before trying a lead again after a failed placement.
HOLD_AFTER_REJECTED = timedelta(minutes=5)
HOLD_AFTER_UNAVAILABLE = timedelta(minutes=1)

# Smallest real sleep between loop turns, so a wake time that is a hair in the
# future cannot make the loop spin.
MIN_SLEEP = timedelta(milliseconds=50)

# Request states
PENDING = "pending"  # place_call activity is running
ACCEPTED_STATE = "accepted"  # provider accepted, waiting for the result
DONE = "done"  # result applied
NOT_PLACED = "not_placed"  # no call was placed


@dataclass
class DispatchInput:
    clock: SimClock
    cfg: CampaignConfig  # passed without the webhook secret: inputs live in history


@dataclass
class LeadOutcome:
    campaign_id: str
    lead_id: str
    final_state: str
    attempts: int
    last_status: str
    last_disposition: str


@dataclass
class DispatchResult:
    leads: list[LeadOutcome]
    counters: dict[str, int] = field(default_factory=dict)  # duplicates seen, rejections...


@dataclass
class DispatchStatus:
    sim_now: str
    leads_total: int
    finished: int
    active_calls: int
    final_states: dict[str, int]
    by_campaign: dict[str, dict[str, int]]
    calls_accepted: int
    counters: dict[str, int] = field(default_factory=dict)


@dataclass
class _Request:
    """What the workflow knows about one request_id."""

    lead_key: str
    status: str = PENDING
    call_sid: str | None = None


def _lead_key(campaign_id: str, lead_id: str) -> str:
    return f"{campaign_id}/{lead_id}"


def _parse_time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


@workflow.defn(name="CampaignDispatcher")
class DispatcherWorkflow:
    def __init__(self) -> None:
        self._cfg: CampaignConfig | None = None
        self._clock: SimClock | None = None
        self._leads: dict[str, LeadState] = {}
        self._dnd_phones: set[str] = set()  # membership checks only, never iterated
        self._dial_seq: dict[str, int] = {}  # lead key -> last dial decision number
        self._current_request: dict[str, str] = {}  # lead key -> its latest request_id
        self._requests: dict[str, _Request] = {}  # request_id -> record
        self._applied_sids: set[str] = set()  # membership checks only
        self._early_results: list[CallResult] = []  # signals that came before run() started
        self._tasks: list[asyncio.Task] = []
        self._wake = False
        self._counters: dict[str, int] = {}

    # ------------------------------------------------------------------ main loop
    @workflow.run
    async def run(self, inp: DispatchInput) -> DispatchResult:
        self._cfg = inp.cfg
        self._clock = inp.clock
        for lead in inp.cfg.leads:
            camp = inp.cfg.campaign(lead.campaign_id)
            key = _lead_key(lead.campaign_id, lead.lead_id)
            self._leads[key] = LeadState(
                campaign_id=lead.campaign_id,
                lead_id=lead.lead_id,
                phone=lead.phone,
                tenant_id=camp.tenant_id,
                max_attempts=camp.max_attempts,
            )
            self._dial_seq[key] = 0
        for res in self._early_results:
            self._apply_result(res)
        self._early_results.clear()

        workflow.logger.info("dispatch started with %d leads", len(self._leads))
        while True:
            sim_now = self._sim_now()
            self._finalise_leads()
            if all(lead.is_finished for lead in self._leads.values()):
                break

            for lead in pick_next(self._leads.values(), self._cfg, sim_now, self._dnd_phones):
                self._start_dial(lead)

            wake_at = next_wake_time(self._leads.values(), self._cfg, sim_now, self._dnd_phones)
            timeout = None
            if wake_at is not None:
                real = self._clock.real_delay_until(wake_at, workflow.now())
                timeout = max(timedelta(seconds=real), MIN_SLEEP)

            self._wake = False
            try:
                await workflow.wait_condition(lambda: self._wake, timeout=timeout)
            except asyncio.TimeoutError:
                pass

        workflow.logger.info("dispatch finished: %s", self._final_state_counts())
        return DispatchResult(leads=self._outcomes(), counters=dict(self._counters))

    # ------------------------------------------------------------------ dialling
    def _start_dial(self, lead: LeadState) -> None:
        """Reserve the lead's slot and phone right now, then place the call in the
        background. The request_id is fixed here, before anything is sent."""
        key = _lead_key(lead.campaign_id, lead.lead_id)
        self._dial_seq[key] += 1
        request_id = f"{lead.campaign_id}:{lead.lead_id}:d{self._dial_seq[key]}"
        lead.active = True
        lead.hold_until = None
        self._requests[request_id] = _Request(lead_key=key)
        self._current_request[key] = request_id
        self._count("dial_decisions")
        self._tasks.append(asyncio.create_task(self._dial(request_id, lead)))

    async def _dial(self, request_id: str, lead: LeadState) -> None:
        outcome: PlaceCallOutcome = await workflow.execute_activity(
            "place_call",
            PlaceCallInput(request_id, lead.campaign_id, lead.lead_id, lead.phone),
            result_type=PlaceCallOutcome,
            start_to_close_timeout=PLACE_CALL_TIMEOUT,
            retry_policy=PLACE_CALL_RETRY,
        )
        req = self._requests[request_id]
        if outcome.outcome == ACCEPTED and outcome.call is not None:
            self._accept(request_id, outcome.call.call_sid)
            if outcome.call.result is not None:  # already ended (a replayed request)
                self._apply_result(outcome.call.result)
        elif req.status == PENDING:
            # Definitely not placed. Free the slot and let the lead be picked again.
            req.status = NOT_PLACED
            lead.active = False
            if outcome.outcome == REJECTED:
                self._count("rejected")
                workflow.logger.error("call %s rejected: %s", request_id, outcome.reason)
                lead.hold_until = self._sim_now() + HOLD_AFTER_REJECTED
            else:
                self._count(f"not_placed_{outcome.reason.split(':')[0] or 'unknown'}")
                if outcome.reason != "window_closed":
                    lead.hold_until = self._sim_now() + HOLD_AFTER_UNAVAILABLE
        self._wake = True

    def _accept(self, request_id: str, call_sid: str) -> None:
        """Count the attempt once, whichever arrives first: the activity's answer or
        the call's result (a webhook can come before we read the 201)."""
        req = self._requests[request_id]
        if req.status in (ACCEPTED_STATE, DONE):
            return
        if req.status == NOT_PLACED:
            # We had concluded "not placed", but the provider has the call after all.
            self._count("late_accepted")
        req.status = ACCEPTED_STATE
        req.call_sid = call_sid
        self._leads[req.lead_key].attempts += 1
        self._count("calls_accepted")

    # ------------------------------------------------------------------ results
    @workflow.signal(name="call_result")
    def call_result(self, res: CallResult) -> None:
        if self._cfg is None:
            self._early_results.append(res)
            return
        self._apply_result(res)
        self._wake = True

    def _apply_result(self, res: CallResult) -> None:
        if res.call_sid in self._applied_sids:
            self._count("duplicate_results")
            return
        req = self._requests.get(res.request_id)
        if req is None:
            self._count("unknown_results")
            workflow.logger.warning("result for unknown request %s ignored", res.request_id)
            return
        self._accept(res.request_id, res.call_sid)
        self._applied_sids.add(res.call_sid)
        req.status = DONE

        lead = self._leads[req.lead_key]
        was_active = lead.active
        record_call_result(lead, res.status, res.disposition, _parse_time(res.ended_at),
                           _parse_time(res.callback_at))
        if self._current_request.get(req.lead_key) != res.request_id:
            # A late result for an older request: the newer call is still in flight.
            lead.active = was_active
        if res.disposition == "dnd":
            self._dnd_phones.add(res.phone)
        self._count("results_applied")
        self._finalise_leads()

    def _finalise_leads(self) -> None:
        for lead in self._leads.values():
            if lead.is_finished:
                continue
            state = resolve_final_state(lead, self._dnd_phones)
            if state is not None:
                lead.final_state = state

    # ------------------------------------------------------------------ queries
    @workflow.query(name="status")
    def status(self) -> DispatchStatus:
        by_campaign: dict[str, dict[str, int]] = {}
        for lead in self._leads.values():
            c = by_campaign.setdefault(lead.campaign_id, {"total": 0, "finished": 0, "active": 0})
            c["total"] += 1
            c["finished"] += lead.is_finished
            c["active"] += lead.active
        return DispatchStatus(
            sim_now=self._sim_now().isoformat() if self._clock else "",
            leads_total=len(self._leads),
            finished=sum(l.is_finished for l in self._leads.values()),
            active_calls=sum(l.active for l in self._leads.values()),
            final_states=self._final_state_counts(),
            by_campaign=by_campaign,
            calls_accepted=self._counters.get("calls_accepted", 0),
            counters=dict(self._counters),
        )

    # ------------------------------------------------------------------ helpers
    def _sim_now(self) -> datetime:
        return self._clock.sim_at(workflow.now())

    def _count(self, name: str) -> None:
        self._counters[name] = self._counters.get(name, 0) + 1

    def _final_state_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for lead in self._leads.values():
            if lead.final_state:
                counts[lead.final_state] = counts.get(lead.final_state, 0) + 1
        return counts

    def _outcomes(self) -> list[LeadOutcome]:
        out = [
            LeadOutcome(
                campaign_id=l.campaign_id,
                lead_id=l.lead_id,
                final_state=l.final_state or "",
                attempts=l.attempts,
                last_status=l.last_status or "",
                last_disposition=l.last_disposition or "",
            )
            for l in self._leads.values()
        ]
        out.sort(key=lambda o: (o.campaign_id, o.lead_id))
        return out
