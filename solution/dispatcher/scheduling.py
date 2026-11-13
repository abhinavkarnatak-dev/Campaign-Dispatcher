"""The scheduling rule book.

Pure functions only: no network, no Temporal, no reading the clock. The caller passes
in the current simulated time and the state of the leads, and gets answers back.
That keeps every rule easy to test with a fake clock, and safe to run inside a
Temporal workflow, which must make the same decisions every time it is replayed.

Rules covered here (numbers from the assignment README):
  R1  calling window (campaign window and the 09:00-21:00 regulatory window)
  R2  per-tenant concurrency, shared across that tenant's campaigns
  R3  never two active calls to the same phone
  R4  at most max_attempts accepted calls per lead
  R5  retry waits after no_answer / busy / failed
  R6  callbacks, and the priority order: due callbacks, then retries, then fresh leads
  R7  terminal dispositions end the lead
  R8  DND blocks the phone in every campaign
  R9  final state precedence: completed > exhausted > dnd_blocked
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

from .clock import IST
from .config import DAYS, Campaign, CampaignConfig, Window

TERMINAL_DISPOSITIONS = frozenset({"interested", "not_interested", "wrong_number", "dnd"})
RETRY_STATUSES = frozenset({"no_answer", "busy", "failed"})

# Stop starting calls this many simulated seconds before a window closes. The
# activity checks the window again right before every request, so the margin only has
# to cover the real time between that check and the provider reading its own clock:
# one local HTTP request. See close_margin_for().
DEFAULT_CLOSE_MARGIN_S = 15
CLOSE_MARGIN_REAL_S = 0.010  # real seconds of request delay the margin covers

# Hold tenant slots for callbacks that become due within this long. The provider's
# longest call is 45 s of ringing plus 6 min of talk, so a call started now could
# still be running when the callback is due.
CALLBACK_LOOKAHEAD = timedelta(minutes=7)

# How far ahead to look for the next open window. A week covers every day pattern.
_LOOKAHEAD_DAYS = 8

# Final states
COMPLETED = "completed"
EXHAUSTED = "exhausted"
DND_BLOCKED = "dnd_blocked"

# Priority groups for R6, lower goes first
PRIORITY_CALLBACK = 0
PRIORITY_RETRY = 1
PRIORITY_FRESH = 2


def close_margin_for(speed: float) -> float:
    """Close margin in simulated seconds for a given clock speed: CLOSE_MARGIN_REAL_S of
    real time, and never less than DEFAULT_CLOSE_MARGIN_S.

    It stays well under a minute at normal speeds, so a callback due at the last whole
    minute of a window (callback_at is always on a whole minute) is still called that
    day instead of the next morning."""
    return max(DEFAULT_CLOSE_MARGIN_S, CLOSE_MARGIN_REAL_S * speed)


# --------------------------------------------------------------------------- windows
def _hm(value: str) -> tuple[int, int]:
    h, m = value.split(":")
    return int(h), int(m)


def _day_bounds(day: datetime, window: Window) -> tuple[datetime, datetime] | None:
    """[start, end) of the window on the IST calendar day of `day`, or None if the
    window is closed that whole day."""
    if window.days is not None and DAYS[day.weekday()] not in window.days:
        return None
    sh, sm = _hm(window.start)
    eh, em = _hm(window.end)
    start = day.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end = day.replace(hour=eh, minute=em, second=0, microsecond=0)
    if end <= start:
        return None
    return start, end


def _allowed_bounds(day: datetime, campaign: Campaign, regulatory: Window,
                    close_margin_s: float) -> tuple[datetime, datetime] | None:
    """Where on this IST day a call may start: the overlap of the campaign window and
    the regulatory window, with the close margin taken off the end."""
    camp = _day_bounds(day, campaign.window)
    reg = _day_bounds(day, regulatory)
    if camp is None or reg is None:
        return None
    start = max(camp[0], reg[0])
    end = min(camp[1], reg[1]) - timedelta(seconds=close_margin_s)
    if end <= start:
        return None
    return start, end


def can_start_call(sim_now: datetime, campaign: Campaign, regulatory: Window,
                   close_margin_s: float = DEFAULT_CLOSE_MARGIN_S) -> bool:
    """R1: may a call for this campaign be started at sim_now?

    Strict: does not use the provider's grace period."""
    t = sim_now.astimezone(IST)
    bounds = _allowed_bounds(t, campaign, regulatory, close_margin_s)
    return bounds is not None and bounds[0] <= t < bounds[1]


def next_allowed_start(sim_t: datetime, campaign: Campaign, regulatory: Window,
                       close_margin_s: float = DEFAULT_CLOSE_MARGIN_S) -> datetime | None:
    """R1 / R6: the earliest moment at or after sim_t when a call may be started.

    Returns sim_t itself if it is already allowed, the next window opening otherwise,
    or None if the windows never open (bad config)."""
    t = sim_t.astimezone(IST)
    for d in range(_LOOKAHEAD_DAYS):
        day = t + timedelta(days=d)
        bounds = _allowed_bounds(day, campaign, regulatory, close_margin_s)
        if bounds is None:
            continue
        start, end = bounds
        if d == 0:
            if t < start:
                return start
            if t < end:
                return t
            continue
        return start
    return None


# --------------------------------------------------------------------------- leads
@dataclass
class LeadState:
    """Everything the dispatcher tracks about one lead.

    Times are simulated times taken from the provider (ended_at, callback_at), never
    from our own clock, so retry and callback rules are measured exactly as R5 and R6
    say.
    """

    campaign_id: str
    lead_id: str
    phone: str
    tenant_id: str
    max_attempts: int
    attempts: int = 0  # calls the provider accepted (R4)
    active: bool = False  # a call for this lead is in flight right now
    last_status: str | None = None  # status of the most recent ended call
    last_disposition: str | None = None
    last_ended_at: datetime | None = None
    callback_at: datetime | None = None  # set when the last disposition was callback_requested
    hold_until: datetime | None = None  # short back-off after a failed attempt to place a call
    final_state: str | None = None  # completed / exhausted / dnd_blocked once finished

    @property
    def key(self) -> tuple[str, str]:
        return (self.campaign_id, self.lead_id)

    @property
    def is_finished(self) -> bool:
        return self.final_state is not None


def record_call_result(lead: LeadState, status: str, disposition: str | None,
                       ended_at: datetime, callback_at: datetime | None) -> None:
    """Apply one ended call's result to the lead. The caller makes sure each call's
    result is applied only once."""
    lead.active = False
    lead.last_status = status
    lead.last_disposition = disposition
    lead.last_ended_at = ended_at
    lead.callback_at = callback_at if disposition == "callback_requested" else None


def resolve_final_state(lead: LeadState, dnd_phones: set[str] | frozenset[str]) -> str | None:
    """R7 / R8 / R9: the lead's final state, or None if it still has work left.

    Precedence is completed > exhausted > dnd_blocked. A lead with a call in flight is
    never final yet, because that call's result can still change the answer."""
    if lead.active:
        return None
    if lead.last_disposition in TERMINAL_DISPOSITIONS:
        return COMPLETED
    if lead.attempts >= lead.max_attempts:
        return EXHAUSTED
    if lead.phone in dnd_phones:
        return DND_BLOCKED
    return None


def earliest_next_call(lead: LeadState, retry_wait_minutes) -> datetime | None:
    """R5 / R6: the earliest simulated time the next attempt may start, ignoring
    windows. None means no wait applies (a fresh lead).

    retry_wait_minutes is a function status -> minutes, e.g. RetryPolicy.wait_minutes."""
    if lead.last_disposition == "callback_requested" and lead.callback_at is not None:
        return lead.callback_at
    if lead.last_status in RETRY_STATUSES and lead.last_ended_at is not None:
        return lead.last_ended_at + timedelta(minutes=retry_wait_minutes(lead.last_status))
    return None


def priority_group(lead: LeadState) -> int:
    """R6: due callbacks first, then retries, then fresh leads."""
    if lead.last_disposition == "callback_requested":
        return PRIORITY_CALLBACK
    if lead.attempts > 0:
        return PRIORITY_RETRY
    return PRIORITY_FRESH


def ready_at(lead: LeadState, cfg: CampaignConfig, sim_now: datetime,
             close_margin_s: float = DEFAULT_CLOSE_MARGIN_S) -> datetime | None:
    """When this lead next becomes callable, taking waits and windows into account.

    Returns a time <= sim_now if it is callable right now, a future time if it has
    to wait, or None if it is finished or in flight."""
    if lead.is_finished or lead.active:
        return None
    t = sim_now
    for not_before in (earliest_next_call(lead, cfg.retry_policy.wait_minutes), lead.hold_until):
        if not_before is not None and not_before > t:
            t = not_before
    return next_allowed_start(t, cfg.campaign(lead.campaign_id), cfg.regulatory_window,
                              close_margin_s)


def pick_next(leads: Iterable[LeadState], cfg: CampaignConfig, sim_now: datetime,
              dnd_phones: set[str] | frozenset[str],
              close_margin_s: float = DEFAULT_CLOSE_MARGIN_S) -> list[LeadState]:
    """Choose which leads to dial right now, in order.

    Applies R1 (window), R2 (tenant limit, shared across campaigns), R3 (one active
    call per phone), R4 (attempts left), R5/R6 (waits and priority), R8 (DND).

    To keep callbacks on time, a tenant's slots are held for callbacks that become
    due within CALLBACK_LOOKAHEAD: retries and fresh leads may not use those slots,
    so a slot is free when the callback is due."""
    leads = list(leads)

    active_by_tenant: dict[str, int] = {}
    busy_phones: set[str] = set()
    for lead in leads:
        if lead.active:
            active_by_tenant[lead.tenant_id] = active_by_tenant.get(lead.tenant_id, 0) + 1
            busy_phones.add(lead.phone)

    candidates: list[tuple[int, datetime, str, str, LeadState]] = []
    held_for_callbacks: dict[str, int] = {}
    for lead in leads:
        if lead.is_finished or lead.active:
            continue
        if lead.attempts >= lead.max_attempts or lead.phone in dnd_phones:
            continue  # will be finalised by resolve_final_state, never dialled
        when = ready_at(lead, cfg, sim_now, close_margin_s)
        if when is None:
            continue
        if when > sim_now:
            if priority_group(lead) == PRIORITY_CALLBACK and when <= sim_now + CALLBACK_LOOKAHEAD:
                held_for_callbacks[lead.tenant_id] = held_for_callbacks.get(lead.tenant_id, 0) + 1
            continue
        due = earliest_next_call(lead, cfg.retry_policy.wait_minutes) or sim_now
        candidates.append((priority_group(lead), due, lead.campaign_id, lead.lead_id, lead))

    candidates.sort(key=lambda c: c[:4])

    chosen: list[LeadState] = []
    for group, *_, lead in candidates:
        limit = cfg.tenant(lead.tenant_id).max_concurrent_calls
        if group != PRIORITY_CALLBACK:
            limit -= held_for_callbacks.get(lead.tenant_id, 0)
        if active_by_tenant.get(lead.tenant_id, 0) >= limit:
            continue
        if lead.phone in busy_phones:
            continue
        chosen.append(lead)
        active_by_tenant[lead.tenant_id] = active_by_tenant.get(lead.tenant_id, 0) + 1
        busy_phones.add(lead.phone)
    return chosen


def next_wake_time(leads: Iterable[LeadState], cfg: CampaignConfig, sim_now: datetime,
                   dnd_phones: set[str] | frozenset[str],
                   close_margin_s: float = DEFAULT_CLOSE_MARGIN_S) -> datetime | None:
    """The earliest future simulated time at which some waiting lead becomes callable.

    Leads that are ready now but blocked by a limit are not counted here: they get
    picked up when a call ends, which wakes the workflow anyway."""
    best: datetime | None = None
    for lead in leads:
        if lead.is_finished or lead.active:
            continue
        if lead.attempts >= lead.max_attempts or lead.phone in dnd_phones:
            continue
        when = ready_at(lead, cfg, sim_now, close_margin_s)
        if when is None or when <= sim_now:
            continue
        if best is None or when < best:
            best = when
    return best
