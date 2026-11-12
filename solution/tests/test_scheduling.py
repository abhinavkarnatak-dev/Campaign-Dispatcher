"""Tests for the scheduling rules, using fixed simulated times (a fake clock).

Calendar used below (IST): Thu 12 Nov 2026, Fri 13, Sat 14, Sun 15, Mon 16.
"""
from datetime import datetime, timedelta, timezone

import pytest

from dispatcher.clock import IST
from dispatcher.config import (
    Campaign,
    CampaignConfig,
    RetryPolicy,
    Tenant,
    Window,
    load_campaign_config,
)
from dispatcher.scheduling import (
    COMPLETED,
    DND_BLOCKED,
    EXHAUSTED,
    PRIORITY_CALLBACK,
    PRIORITY_FRESH,
    PRIORITY_RETRY,
    LeadState,
    can_start_call,
    earliest_next_call,
    next_allowed_start,
    next_wake_time,
    pick_next,
    priority_group,
    record_call_result,
    resolve_final_state,
)

REG = Window("09:00", "21:00")
WEEKDAYS = ("mon", "tue", "wed", "thu", "fri")
ALL_DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


def ist(day: int, h: int, m: int = 0, s: int = 0) -> datetime:
    return datetime(2026, 11, day, h, m, s, tzinfo=IST)


def campaign(cid="c1", tenant="t1", start="10:00", end="19:00", days=WEEKDAYS, attempts=3):
    return Campaign(campaign_id=cid, tenant_id=tenant, window=Window(start, end, days),
                    max_attempts=attempts)


def make_cfg(campaigns, tenants=(Tenant("t1", 2),)) -> CampaignConfig:
    return CampaignConfig(
        regulatory_window=REG,
        window_grace_seconds=120,
        retry_policy=RetryPolicy(no_answer_minutes=60, busy_minutes=60, failed_minutes=15),
        webhook_secret="s",
        tenants=tuple(tenants),
        campaigns=tuple(campaigns),
    )


def lead(lid="l1", cid="c1", phone="+919000000001", tenant="t1", attempts=0, **kw) -> LeadState:
    return LeadState(campaign_id=cid, lead_id=lid, phone=phone, tenant_id=tenant,
                     max_attempts=3, attempts=attempts, **kw)


# --------------------------------------------------------------------------- R1 windows
class TestCanStartCall:
    def test_inside_window(self):
        assert can_start_call(ist(12, 12), campaign(), REG)

    def test_exactly_at_open(self):
        assert can_start_call(ist(12, 10), campaign(), REG)

    def test_before_open(self):
        assert not can_start_call(ist(12, 9, 59, 59), campaign(), REG)

    def test_at_close_is_not_allowed(self):
        assert not can_start_call(ist(12, 19), campaign(), REG)

    def test_close_margin_blocks_last_minute(self):
        assert can_start_call(ist(12, 18, 58, 59), campaign(), REG)
        assert not can_start_call(ist(12, 18, 59, 0), campaign(), REG)
        assert not can_start_call(ist(12, 18, 59, 30), campaign(), REG)

    def test_zero_margin_allows_up_to_close(self):
        assert can_start_call(ist(12, 18, 59, 59), campaign(), REG, close_margin_s=0)

    def test_wrong_day(self):
        assert not can_start_call(ist(14, 12), campaign(days=WEEKDAYS), REG)  # Saturday

    def test_regulatory_window_cuts_campaign_window(self):
        late = campaign(start="08:00", end="22:00", days=ALL_DAYS)
        assert not can_start_call(ist(12, 8, 30), late, REG)
        assert can_start_call(ist(12, 9, 0), late, REG)
        assert not can_start_call(ist(12, 21, 0), late, REG)
        assert not can_start_call(ist(12, 20, 59, 30), late, REG)  # margin before 21:00

    def test_utc_input_is_converted_to_ist(self):
        # 06:30 UTC is 12:00 IST
        assert can_start_call(datetime(2026, 11, 12, 6, 30, tzinfo=timezone.utc), campaign(), REG)


class TestNextAllowedStart:
    def test_now_if_already_open(self):
        assert next_allowed_start(ist(12, 12), campaign(), REG) == ist(12, 12)

    def test_later_today_if_before_open(self):
        assert next_allowed_start(ist(12, 8), campaign(), REG) == ist(12, 10)

    def test_next_day_if_after_close(self):
        assert next_allowed_start(ist(12, 19, 30), campaign(), REG) == ist(13, 10)

    def test_inside_close_margin_rolls_to_next_day(self):
        assert next_allowed_start(ist(12, 18, 59, 30), campaign(), REG) == ist(13, 10)

    def test_skips_weekend(self):
        # Friday evening -> Monday morning for a weekday-only campaign
        assert next_allowed_start(ist(13, 20), campaign(days=WEEKDAYS), REG) == ist(16, 10)

    def test_regulatory_open_wins_over_earlier_campaign_open(self):
        early = campaign(start="07:00", end="18:00", days=ALL_DAYS)
        assert next_allowed_start(ist(12, 6), early, REG) == ist(12, 9)

    def test_never_open_returns_none(self):
        never = campaign(start="22:00", end="23:00", days=ALL_DAYS)
        assert next_allowed_start(ist(12, 12), never, REG) is None


# --------------------------------------------------------------------------- R5 / R6 waits
class TestEarliestNextCall:
    policy = RetryPolicy(no_answer_minutes=60, busy_minutes=60, failed_minutes=15)

    def test_fresh_lead_has_no_wait(self):
        assert earliest_next_call(lead(), self.policy.wait_minutes) is None

    @pytest.mark.parametrize("status,minutes", [("no_answer", 60), ("busy", 60), ("failed", 15)])
    def test_retry_wait_from_ended_at(self, status, minutes):
        l = lead(attempts=1)
        record_call_result(l, status, None, ist(12, 11, 7), None)
        assert earliest_next_call(l, self.policy.wait_minutes) == ist(12, 11, 7) + timedelta(minutes=minutes)

    def test_callback_uses_callback_time(self):
        l = lead(attempts=1)
        record_call_result(l, "completed", "callback_requested", ist(12, 11), ist(12, 14, 15))
        assert earliest_next_call(l, self.policy.wait_minutes) == ist(12, 14, 15)

    def test_callback_time_ignored_for_other_dispositions(self):
        l = lead(attempts=1)
        record_call_result(l, "completed", "not_interested", ist(12, 11), ist(12, 14, 15))
        assert l.callback_at is None


class TestReadyAndWake:
    def setup_method(self):
        self.cfg = make_cfg([campaign()])

    def test_callback_outside_window_moves_to_next_opening(self):
        l = lead(attempts=1)
        record_call_result(l, "completed", "callback_requested", ist(12, 18), ist(12, 20, 15))
        assert next_wake_time([l], self.cfg, ist(12, 18, 5), set()) == ist(13, 10)

    def test_retry_inside_window_wakes_at_exact_time(self):
        l = lead(attempts=1)
        record_call_result(l, "no_answer", None, ist(12, 11, 0, 40), None)
        assert next_wake_time([l], self.cfg, ist(12, 11, 1), set()) == ist(12, 12, 0, 40)

    def test_not_picked_before_wait_is_over(self):
        l = lead(attempts=1)
        record_call_result(l, "busy", None, ist(12, 11), None)
        assert pick_next([l], self.cfg, ist(12, 11, 59, 59), set()) == []
        assert pick_next([l], self.cfg, ist(12, 12), set()) == [l]


class TestHoldUntil:
    def setup_method(self):
        self.cfg = make_cfg([campaign()])

    def test_hold_delays_a_fresh_lead(self):
        l = lead(hold_until=ist(12, 12, 5))
        assert pick_next([l], self.cfg, ist(12, 12), set()) == []
        assert next_wake_time([l], self.cfg, ist(12, 12), set()) == ist(12, 12, 5)
        assert pick_next([l], self.cfg, ist(12, 12, 5), set()) == [l]

    def test_later_of_hold_and_retry_wait_wins(self):
        l = lead(attempts=1)
        record_call_result(l, "failed", None, ist(12, 11), None)  # retry from 11:15
        l.hold_until = ist(12, 11, 20)
        assert next_wake_time([l], self.cfg, ist(12, 11, 1), set()) == ist(12, 11, 20)


# --------------------------------------------------------------------------- priority
class TestPriority:
    def test_groups(self):
        fresh = lead("a")
        retry = lead("b", attempts=1)
        record_call_result(retry, "no_answer", None, ist(12, 10), None)
        cb = lead("c", attempts=1)
        record_call_result(cb, "completed", "callback_requested", ist(12, 10), ist(12, 10, 30))
        assert priority_group(cb) == PRIORITY_CALLBACK
        assert priority_group(retry) == PRIORITY_RETRY
        assert priority_group(fresh) == PRIORITY_FRESH

    def test_callbacks_then_retries_then_fresh(self):
        cfg = make_cfg([campaign()], tenants=[Tenant("t1", 10)])
        fresh = lead("a", phone="+919000000001")
        retry = lead("b", phone="+919000000002", attempts=1)
        record_call_result(retry, "failed", None, ist(12, 10), None)
        cb = lead("c", phone="+919000000003", attempts=1)
        record_call_result(cb, "completed", "callback_requested", ist(12, 10), ist(12, 11))
        picked = pick_next([fresh, retry, cb], cfg, ist(12, 12), set())
        assert [l.lead_id for l in picked] == ["c", "b", "a"]

    def test_limit_goes_to_higher_priority(self):
        cfg = make_cfg([campaign()], tenants=[Tenant("t1", 1)])
        fresh = lead("a", phone="+919000000001")
        cb = lead("z", phone="+919000000003", attempts=1)
        record_call_result(cb, "completed", "callback_requested", ist(12, 10), ist(12, 11))
        assert pick_next([fresh, cb], cfg, ist(12, 12), set()) == [cb]

    def test_older_callback_first(self):
        cfg = make_cfg([campaign()], tenants=[Tenant("t1", 1)])
        late = lead("a", phone="+919000000001", attempts=1)
        record_call_result(late, "completed", "callback_requested", ist(12, 10), ist(12, 11, 30))
        early = lead("b", phone="+919000000002", attempts=1)
        record_call_result(early, "completed", "callback_requested", ist(12, 10), ist(12, 11))
        assert pick_next([late, early], cfg, ist(12, 12), set()) == [early]


# --------------------------------------------------------------------------- R2 / R3 / R4 / R8 limits
class TestLimits:
    def test_tenant_limit_is_shared_across_campaigns(self):
        cfg = make_cfg([campaign("c1"), campaign("c2")], tenants=[Tenant("t1", 2)])
        busy = lead("x", cid="c1", phone="+919000000009", active=True)
        a = lead("a", cid="c1", phone="+919000000001")
        b = lead("b", cid="c2", phone="+919000000002")
        assert len(pick_next([busy, a, b], cfg, ist(12, 12), set())) == 1

    def test_tenants_do_not_share_limits(self):
        cfg = make_cfg([campaign("c1", "t1"), campaign("c2", "t2")],
                       tenants=[Tenant("t1", 1), Tenant("t2", 1)])
        a = lead("a", cid="c1", tenant="t1", phone="+919000000001")
        b = lead("b", cid="c2", tenant="t2", phone="+919000000002")
        assert len(pick_next([a, b], cfg, ist(12, 12), set())) == 2

    def test_same_phone_never_twice_at_once(self):
        cfg = make_cfg([campaign("c1"), campaign("c2")], tenants=[Tenant("t1", 5)])
        a = lead("a", cid="c1", phone="+919000000001")
        b = lead("b", cid="c2", phone="+919000000001")
        assert len(pick_next([a, b], cfg, ist(12, 12), set())) == 1

    def test_phone_busy_in_other_campaign_blocks(self):
        cfg = make_cfg([campaign("c1"), campaign("c2")], tenants=[Tenant("t1", 5)])
        busy = lead("a", cid="c1", phone="+919000000001", active=True)
        b = lead("b", cid="c2", phone="+919000000001")
        assert pick_next([busy, b], cfg, ist(12, 12), set()) == []

    def test_no_dial_after_max_attempts(self):
        cfg = make_cfg([campaign()])
        l = lead(attempts=3)
        record_call_result(l, "no_answer", None, ist(12, 10), None)
        assert pick_next([l], cfg, ist(12, 15), set()) == []

    def test_dnd_phone_never_dialled(self):
        cfg = make_cfg([campaign()])
        l = lead(phone="+919000000001")
        assert pick_next([l], cfg, ist(12, 12), {"+919000000001"}) == []

    def test_outside_window_nothing_picked(self):
        cfg = make_cfg([campaign()])
        assert pick_next([lead()], cfg, ist(12, 20), set()) == []

    def test_finished_and_active_leads_skipped(self):
        cfg = make_cfg([campaign()])
        done = lead("a", phone="+919000000001", final_state=COMPLETED)
        flying = lead("b", phone="+919000000002", active=True)
        assert pick_next([done, flying], cfg, ist(12, 12), set()) == []


# --------------------------------------------------------------------------- R7 / R8 / R9 final state
class TestFinalState:
    @pytest.mark.parametrize("disp", ["interested", "not_interested", "wrong_number", "dnd"])
    def test_terminal_disposition_completes(self, disp):
        l = lead(attempts=1)
        record_call_result(l, "completed", disp, ist(12, 11), None)
        assert resolve_final_state(l, set()) == COMPLETED

    def test_exhausted_after_max_attempts(self):
        l = lead(attempts=3)
        record_call_result(l, "no_answer", None, ist(12, 11), None)
        assert resolve_final_state(l, set()) == EXHAUSTED

    def test_callback_on_last_attempt_is_exhausted(self):
        l = lead(attempts=3)
        record_call_result(l, "completed", "callback_requested", ist(12, 11), ist(12, 13))
        assert resolve_final_state(l, set()) == EXHAUSTED

    def test_dnd_blocked_when_phone_opted_out_elsewhere(self):
        l = lead(attempts=1, phone="+919000000001")
        record_call_result(l, "no_answer", None, ist(12, 11), None)
        assert resolve_final_state(l, {"+919000000001"}) == DND_BLOCKED

    def test_dnd_blocked_with_no_attempts(self):
        assert resolve_final_state(lead(phone="+919000000001"), {"+919000000001"}) == DND_BLOCKED

    def test_completed_beats_dnd_blocked(self):
        l = lead(attempts=1, phone="+919000000001")
        record_call_result(l, "completed", "interested", ist(12, 11), None)
        assert resolve_final_state(l, {"+919000000001"}) == COMPLETED

    def test_exhausted_beats_dnd_blocked(self):
        l = lead(attempts=3, phone="+919000000001")
        record_call_result(l, "busy", None, ist(12, 11), None)
        assert resolve_final_state(l, {"+919000000001"}) == EXHAUSTED

    def test_active_lead_is_never_final(self):
        l = lead(attempts=3, active=True, phone="+919000000001")
        assert resolve_final_state(l, {"+919000000001"}) is None

    def test_lead_with_work_left_is_not_final(self):
        l = lead(attempts=1)
        record_call_result(l, "no_answer", None, ist(12, 11), None)
        assert resolve_final_state(l, set()) is None


# --------------------------------------------------------------------------- real fixtures
def test_real_fixtures_first_pick_respects_tenant_limits():
    """At the sim start (Thu 17:30) all three campaigns are open. The picks must fit
    each tenant's limit, and no phone may be picked twice."""
    cfg = load_campaign_config()
    leads = [LeadState(campaign_id=l.campaign_id, lead_id=l.lead_id, phone=l.phone,
                       tenant_id=cfg.campaign(l.campaign_id).tenant_id,
                       max_attempts=cfg.campaign(l.campaign_id).max_attempts)
             for l in cfg.leads]
    picked = pick_next(leads, cfg, ist(12, 17, 30), set())
    per_tenant: dict[str, int] = {}
    for l in picked:
        per_tenant[l.tenant_id] = per_tenant.get(l.tenant_id, 0) + 1
    assert per_tenant == {"acme_insurance": 4, "zen_retail": 3}
    assert len({l.phone for l in picked}) == len(picked)
