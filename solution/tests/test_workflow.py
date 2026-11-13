"""Workflow tests against a real local Temporal server.

The place_call activity is replaced by a fake provider that also acts as a referee:
it records every call, sends results back as signals (sometimes early, sometimes
twice), and flags any rule we break.

Replay is checked three ways: the small scenarios run with max_cached_workflows=0,
so Temporal replays the whole history on every single step; the full run swaps in a
brand new worker halfway, which must rebuild everything from history; and every run
ends by replaying its complete history offline with Temporal's Replayer.

The first run downloads Temporal's dev server (about 50 MB). If the server cannot
start (for example with no network), these tests are skipped.
"""
from __future__ import annotations

import asyncio
import hashlib
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from temporalio import activity
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from dispatcher.activities import PlaceCallInput
from dispatcher.clock import IST, SimClock
from dispatcher.config import Campaign, CampaignConfig, Lead, RetryPolicy, Tenant, Window, load_campaign_config
from dispatcher.provider import ACCEPTED, CallInfo, CallResult, PlaceCallOutcome
from dispatcher.scheduling import TERMINAL_DISPOSITIONS
from dispatcher.workflow import DispatchInput, DispatchResult, DispatcherWorkflow

SPEED = 36_000.0  # 1 real second = 10 simulated hours, so a full run takes seconds
ALL_DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


class FakeProvider:
    """Stands in for the telephony provider and checks every rule it can see."""

    def __init__(self, cfg: CampaignConfig, clock: SimClock, script, *,
                 early_every: int = 0, duplicate_every: int = 0):
        self.cfg = cfg
        self.clock = clock
        self.script = script  # (campaign_id, lead_id, attempt_no) -> (status, disposition)
        self.early_every = early_every
        self.duplicate_every = duplicate_every
        self.client: Client | None = None
        self.workflow_id = ""
        self.by_request: dict[str, CallInfo] = {}
        self.attempts: dict[tuple[str, str], int] = {}
        self.last_end: dict[tuple[str, str], tuple[str, str | None]] = {}
        self.terminal: set[tuple[str, str]] = set()
        self.dnd: set[str] = set()
        self.active_tenant: dict[str, int] = {}
        self.active_phone: set[str] = set()
        self.max_seen: dict[str, int] = {}
        self.violations: list[str] = []
        self.calls = 0
        self._tasks: list[asyncio.Task] = []

    def _sim_now(self) -> datetime:
        return self.clock.sim_at(datetime.now(timezone.utc))

    @activity.defn(name="place_call")
    async def place_call(self, inp: PlaceCallInput) -> PlaceCallOutcome:
        if inp.request_id in self.by_request:  # idempotent replay
            return PlaceCallOutcome(ACCEPTED, call=self.by_request[inp.request_id])
        key = (inp.campaign_id, inp.lead_id)
        camp = self.cfg.campaign(inp.campaign_id)
        tenant = camp.tenant_id
        if key in self.terminal:
            self.violations.append(f"dial_after_terminal {key}")
        if inp.phone in self.dnd:
            self.violations.append(f"dnd_violation {key}")
        if self.attempts.get(key, 0) >= camp.max_attempts:
            self.violations.append(f"max_attempts_exceeded {key}")
        if inp.phone in self.active_phone:
            self.violations.append(f"concurrent_dial_same_phone {key}")
        if self.active_tenant.get(tenant, 0) >= self.cfg.tenant(tenant).max_concurrent_calls:
            self.violations.append(f"tenant_concurrency_exceeded {tenant}")

        self.calls += 1
        self.attempts[key] = self.attempts.get(key, 0) + 1
        self.active_tenant[tenant] = self.active_tenant.get(tenant, 0) + 1
        self.max_seen[tenant] = max(self.max_seen.get(tenant, 0), self.active_tenant[tenant])
        self.active_phone.add(inp.phone)
        sid = "CA" + uuid.uuid4().hex[:12]
        info = CallInfo(call_sid=sid, request_id=inp.request_id, state="active",
                        created_at=self._sim_now().isoformat())
        self.by_request[inp.request_id] = info

        status, disp = self.script(inp.campaign_id, inp.lead_id, self.attempts[key])
        early = self.early_every and self.calls % self.early_every == 0
        if early:  # the call ends and the webhook arrives before we return the 201
            # Shielded: a worker shutdown cancels this activity, but the provider's
            # call (and its webhook) carries on regardless, as with the real provider.
            task = asyncio.create_task(self._finish(inp, tenant, info, status, disp))
            self._tasks.append(task)
            await asyncio.shield(task)
        else:
            self._tasks.append(asyncio.create_task(self._finish(inp, tenant, info, status, disp, delay=0.02)))
        return PlaceCallOutcome(ACCEPTED, call=info)

    async def _finish(self, inp, tenant, info, status, disp, delay=0.0):
        await asyncio.sleep(delay)
        key = (inp.campaign_id, inp.lead_id)
        ended = self._sim_now()
        callback_at = None
        if disp == "callback_requested":
            callback_at = (ended.replace(second=0, microsecond=0) + timedelta(minutes=45)).isoformat()
        self.active_tenant[tenant] -= 1
        self.active_phone.discard(inp.phone)
        self.last_end[key] = (status, disp)
        if disp in TERMINAL_DISPOSITIONS:
            self.terminal.add(key)
        if disp == "dnd":
            self.dnd.add(inp.phone)
        res = CallResult(call_sid=info.call_sid, request_id=inp.request_id,
                         campaign_id=inp.campaign_id, lead_id=inp.lead_id, phone=inp.phone,
                         status=status, disposition=disp, callback_at=callback_at,
                         ended_at=ended.isoformat())
        handle = self.client.get_workflow_handle(self.workflow_id)
        await handle.signal("call_result", res)
        if self.duplicate_every and self.calls % self.duplicate_every == 0:
            await handle.signal("call_result", res)

    def expected_states(self) -> dict[tuple[str, str], str]:
        """Same precedence the real provider uses in /admin/leads."""
        out = {}
        for lead in self.cfg.leads:
            key = (lead.campaign_id, lead.lead_id)
            if key in self.terminal:
                out[key] = "completed"
            elif self.attempts.get(key, 0) >= self.cfg.campaign(lead.campaign_id).max_attempts:
                out[key] = "exhausted"
            elif lead.phone in self.dnd:
                out[key] = "dnd_blocked"
            else:
                out[key] = "incomplete"
        return out


def hashed_script(seed: str):
    """Deterministic mixed outcomes, similar in spirit to the real provider."""
    def script(cid, lid, attempt):
        h = int(hashlib.sha256(f"{seed}:{cid}:{lid}:{attempt}".encode()).hexdigest()[:8], 16) / 0xFFFFFFFF
        if h < 0.10:
            return ("failed", None)
        if h < 0.35:
            return ("no_answer", None)
        if h < 0.42:
            return ("busy", None)
        for limit, disp in ((0.58, "interested"), (0.70, "not_interested"),
                            (0.88, "callback_requested"), (0.94, "wrong_number"), (1.01, "dnd")):
            if h < limit:
                return ("completed", disp)
    return script


def make_clock() -> SimClock:
    start = datetime(2026, 11, 12, 12, 0, tzinfo=IST)  # Thursday noon
    return SimClock(sim_epoch_iso=start.isoformat(),
                    real_epoch=datetime.now(timezone.utc).timestamp(), speed=SPEED)


async def run_dispatch(cfg: CampaignConfig, fake: FakeProvider, *, swap_worker_after: int = 0,
                       replay_every_step: bool = False, timeout: float = 120.0) -> tuple[DispatchResult, Client, str]:
    try:
        env = await WorkflowEnvironment.start_local()
    except Exception as exc:  # pragma: no cover - depends on the machine
        pytest.skip(f"Temporal dev server unavailable: {exc}")
    async with env:
        client = env.client
        wid = f"dispatch-{uuid.uuid4().hex[:8]}"
        fake.client, fake.workflow_id = client, wid
        task_queue = f"tq-{wid}"

        def new_worker():
            return Worker(client, task_queue=task_queue, workflows=[DispatcherWorkflow],
                          activities=[fake.place_call],
                          max_cached_workflows=0 if replay_every_step else 100)

        worker = new_worker()
        worker_task = asyncio.create_task(worker.run())
        handle = await client.start_workflow(
            DispatcherWorkflow.run, DispatchInput(clock=fake.clock, cfg=cfg),
            id=wid, task_queue=task_queue)

        if swap_worker_after:
            while fake.calls < swap_worker_after:
                await asyncio.sleep(0.05)
            await worker.shutdown()  # the "crash": this worker is gone
            await worker_task
            worker = new_worker()  # a brand new worker picks up from history
            worker_task = asyncio.create_task(worker.run())

        result = await asyncio.wait_for(handle.result(), timeout=timeout)
        history = await handle.fetch_history()
        await worker.shutdown()
        await worker_task
        # Replay the full history offline: raises if any decision would differ.
        await Replayer(workflows=[DispatcherWorkflow]).replay_workflow(history)
        return result, client, wid


def assert_matches_provider(result: DispatchResult, fake: FakeProvider):
    assert fake.violations == []
    expected = fake.expected_states()
    got = {(o.campaign_id, o.lead_id): o.final_state for o in result.leads}
    assert "incomplete" not in expected.values()
    assert got == expected
    for o in result.leads:
        assert o.attempts == fake.attempts.get((o.campaign_id, o.lead_id), 0)
        last = fake.last_end.get((o.campaign_id, o.lead_id))
        assert (o.last_status, o.last_disposition) == ((last[0], last[1] or "") if last else ("", ""))


# --------------------------------------------------------------------------- small scenarios
def small_cfg() -> CampaignConfig:
    win = Window("09:00", "21:00", ALL_DAYS)
    return CampaignConfig(
        regulatory_window=Window("09:00", "21:00"),
        window_grace_seconds=120,
        retry_policy=RetryPolicy(60, 60, 15),
        webhook_secret="",
        tenants=(Tenant("t1", 2),),
        campaigns=(Campaign("c1", "t1", win, 3), Campaign("c2", "t1", win, 3)),
        leads=(
            Lead("c1", "win", "+919000000001"),
            Lead("c1", "noans", "+919000000002"),
            Lead("c1", "cb", "+919000000003"),
            Lead("c1", "dnd", "+919000000004"),
            Lead("c2", "samephone", "+919000000004"),  # same phone as c1/dnd
            Lead("c2", "fail", "+919000000005"),
        ),
    )


SMALL_SCRIPT = {
    ("c1", "win"): [("completed", "interested")],
    ("c1", "noans"): [("no_answer", None), ("busy", None), ("no_answer", None)],
    ("c1", "cb"): [("completed", "callback_requested"), ("completed", "not_interested")],
    ("c1", "dnd"): [("completed", "dnd")],
    ("c2", "samephone"): [("no_answer", None)] * 3,
    ("c2", "fail"): [("failed", None), ("completed", "wrong_number")],
}


def small_script(cid, lid, attempt):
    return SMALL_SCRIPT[(cid, lid)][attempt - 1]


def test_small_campaign_runs_to_the_end():
    cfg = small_cfg()
    fake = FakeProvider(cfg, make_clock(), small_script)
    result, _, _ = asyncio.run(run_dispatch(cfg, fake, replay_every_step=True))
    assert_matches_provider(result, fake)
    states = {o.lead_id: (o.final_state, o.attempts) for o in result.leads}
    assert states["win"] == ("completed", 1)
    assert states["noans"] == ("exhausted", 3)
    assert states["cb"] == ("completed", 2)
    assert states["dnd"] == ("completed", 1)
    assert states["fail"] == ("completed", 2)
    # the shared phone opted out: the c2 lead is blocked, unless it ran out first
    assert states["samephone"][0] in ("dnd_blocked", "exhausted")
    assert fake.max_seen["t1"] <= 2


def test_early_and_duplicate_results_are_handled():
    cfg = small_cfg()
    fake = FakeProvider(cfg, make_clock(), small_script, early_every=2, duplicate_every=1)
    result, _, _ = asyncio.run(run_dispatch(cfg, fake, replay_every_step=True))
    assert_matches_provider(result, fake)
    # every result was sent twice: each duplicate must have been seen and ignored
    assert result.counters["duplicate_results"] == fake.calls
    assert result.counters["calls_accepted"] == fake.calls


# --------------------------------------------------------------------------- full fixtures
def real_cfg() -> CampaignConfig:
    return replace(load_campaign_config(), webhook_secret="")


def test_full_fixtures_with_worker_crash_midway():
    """All 200 real leads, mixed outcomes, early and duplicate results, and the
    worker replaced partway through. Nothing lost, nothing double-dialled, no rule
    broken, and every final state matches the provider's own view."""
    cfg = real_cfg()
    fake = FakeProvider(cfg, make_clock(), hashed_script("t1"), early_every=7, duplicate_every=5)
    result, _, _ = asyncio.run(run_dispatch(cfg, fake, swap_worker_after=60))
    assert_matches_provider(result, fake)
    assert len(result.leads) == 200
    assert fake.max_seen["acme_insurance"] <= 4 and fake.max_seen["zen_retail"] <= 3
    assert result.counters["calls_accepted"] == fake.calls
    assert result.counters["duplicate_results"] > 0
