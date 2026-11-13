"""Tests for the provider client and activities, with a fake provider.

httpx.MockTransport stands in for the provider, so every response (201, 200, 503,
504, rule violations, network errors) can be scripted without running anything.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from temporalio.converter import DataConverter

from dispatcher.activities import DispatcherActivities, PlaceCallInput
from dispatcher.clock import IST, SimClock
from dispatcher.config import load_campaign_config
from dispatcher.provider import ACCEPTED, NOT_PLACED, REJECTED, CallInfo, PlaceCallOutcome, ProviderClient

BASE = "http://provider.test"
CALL = {
    "call_sid": "CA123",
    "request_id": "c1:l1:d1",
    "campaign_id": "c1",
    "lead_id": "l1",
    "state": "active",
    "created_at": "2026-11-12T17:31:00+05:30",
}
ENDED = dict(CALL, state="ended", result={
    "event": "call.completed", "call_sid": "CA123", "request_id": "c1:l1:d1",
    "tenant_id": "t1", "campaign_id": "c1", "lead_id": "l1", "phone": "+919000000001",
    "status": "completed", "disposition": "callback_requested",
    "callback_at": "2026-11-12T19:00:00+05:30", "started_at": "2026-11-12T17:31:00+05:30",
    "ended_at": "2026-11-12T17:35:00+05:30", "duration_sec": 200,
})


class FakeProvider:
    """Scripted POST responses, plus a set of request_ids the provider knows about."""

    def __init__(self, post_script, known=()):
        self.post_script = list(post_script)
        self.known = set(known)
        self.posts = 0
        self.lookups = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            self.posts += 1
            step = self.post_script.pop(0)
            if isinstance(step, Exception):
                raise step
            code, places = step
            if places:
                self.known.add("c1:l1:d1")
            body = CALL if code in (200, 201) else {"detail": {"error": f"code_{code}"}}
            return httpx.Response(code, json=body)
        if request.url.path == "/v1/calls":
            self.lookups += 1
            if request.url.params["request_id"] in self.known:
                return httpx.Response(200, json=CALL)
            return httpx.Response(404, json={"detail": {"error": "not_found"}})
        if request.url.path == "/v1/calls/CA123":
            return httpx.Response(200, json=ENDED)
        return httpx.Response(404, json={"detail": {"error": "not_found"}})


async def _no_sleep(_):
    return None


def place(fake: FakeProvider, may_send=lambda: True) -> PlaceCallOutcome:
    async def run():
        client = ProviderClient(BASE, httpx.AsyncClient(transport=httpx.MockTransport(fake.handler)))
        try:
            return await client.place_call(
                request_id="c1:l1:d1", campaign_id="c1", lead_id="l1",
                phone="+919000000001", callback_url="http://cb.test/webhooks/telephony",
                may_send=may_send, sleep=_no_sleep)
        finally:
            await client.aclose()
    return asyncio.run(run())


# --------------------------------------------------------------------------- place_call
def test_201_is_accepted():
    fake = FakeProvider([(201, True)])
    out = place(fake)
    assert out.outcome == ACCEPTED and out.call.call_sid == "CA123"
    assert fake.posts == 1


def test_200_replay_is_accepted():
    out = place(FakeProvider([(200, False)]))
    assert out.outcome == ACCEPTED


def test_503_then_201_retries_with_same_request():
    fake = FakeProvider([(503, False), (503, False), (201, True)])
    assert place(fake).outcome == ACCEPTED
    assert fake.posts == 3


def test_503_every_time_gives_not_placed():
    fake = FakeProvider([(503, False)] * 5)
    out = place(fake)
    assert out.outcome == NOT_PLACED and out.reason == "unavailable"
    assert fake.posts == 5


def test_504_but_call_was_placed_is_accepted_without_resending():
    fake = FakeProvider([(504, True)])
    out = place(fake)
    assert out.outcome == ACCEPTED
    assert fake.posts == 1 and fake.lookups == 1


def test_504_not_placed_then_retry_succeeds():
    fake = FakeProvider([(504, False), (201, True)])
    assert place(fake).outcome == ACCEPTED
    assert fake.posts == 2


def test_network_error_but_call_was_placed():
    # The request reached the provider, but the connection dropped before the reply.
    class PlacedThenDropped(FakeProvider):
        def handler(self, request):
            if request.method == "POST":
                self.posts += 1
                self.known.add("c1:l1:d1")
                raise httpx.ReadTimeout("lost reply")
            return super().handler(request)

    fake = PlacedThenDropped([])
    assert place(fake).outcome == ACCEPTED
    assert fake.posts == 1


@pytest.mark.parametrize("code", [409, 422, 429])
def test_rule_violation_is_rejected_and_not_retried(code):
    fake = FakeProvider([(code, False)])
    out = place(fake)
    assert out.outcome == REJECTED and out.reason.startswith(str(code))
    assert fake.posts == 1


def test_window_closed_sends_nothing():
    fake = FakeProvider([])
    out = place(fake, may_send=lambda: False)
    assert out.outcome == NOT_PLACED and out.reason == "window_closed"
    assert fake.posts == 0


def test_window_closes_after_unknown_attempt_checks_before_giving_up():
    # 504 and the call was NOT placed, then the window closes before the retry.
    # The client must look once more before saying not_placed.
    fake = FakeProvider([(504, False)])
    sends = iter([True, False])
    out = place(fake, may_send=lambda: next(sends))
    assert out.outcome == NOT_PLACED and out.reason == "window_closed"
    assert fake.posts == 1 and fake.lookups == 2


def test_window_closes_after_504_that_placed_late_is_accepted():
    # 504, lookup says not found, then the call shows up (placed late). The window
    # closes, but the final lookup finds the call, so it is counted, not lost.
    class LatePlacement(FakeProvider):
        def handler(self, request):
            if request.method == "GET" and self.lookups == 1:
                self.known.add("c1:l1:d1")
            return super().handler(request)

    fake = LatePlacement([(504, False)])
    sends = iter([True, False])
    assert place(fake, may_send=lambda: next(sends)).outcome == ACCEPTED


def test_failing_lookup_raises_so_the_activity_is_retried():
    def handler(request):
        if request.method == "POST":
            return httpx.Response(504, json={})
        raise httpx.ConnectError("provider down")

    async def run():
        client = ProviderClient(BASE, httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        try:
            await client.place_call(request_id="r", campaign_id="c1", lead_id="l1", phone="p",
                                    callback_url="u", sleep=_no_sleep)
        finally:
            await client.aclose()

    with pytest.raises(httpx.ConnectError):
        asyncio.run(run())


def test_on_step_runs_before_every_request():
    # 503, then 504 (lookup: not found), then 201: three POSTs and one lookup.
    fake = FakeProvider([(503, False), (504, False), (201, True)])
    steps = []

    async def run():
        client = ProviderClient(BASE, httpx.AsyncClient(transport=httpx.MockTransport(fake.handler)))
        try:
            return await client.place_call(
                request_id="c1:l1:d1", campaign_id="c1", lead_id="l1", phone="+919000000001",
                callback_url="u", on_step=lambda: steps.append(1), sleep=_no_sleep)
        finally:
            await client.aclose()

    assert asyncio.run(run()).outcome == ACCEPTED
    assert len(steps) == fake.posts + fake.lookups == 4


# --------------------------------------------------------------------------- lookups
def test_get_call_returns_result_once_ended():
    async def run():
        client = ProviderClient(BASE, httpx.AsyncClient(transport=httpx.MockTransport(FakeProvider([]).handler)))
        try:
            return await client.get_call("CA123"), await client.get_call("CA999")
        finally:
            await client.aclose()

    found, missing = asyncio.run(run())
    assert found.state == "ended"
    assert found.result.disposition == "callback_requested"
    assert found.result.callback_at == "2026-11-12T19:00:00+05:30"
    assert missing is None


# --------------------------------------------------------------------------- activities
def _activities(sim_now: datetime) -> DispatcherActivities:
    real_now = datetime.now(timezone.utc).timestamp()
    clock = SimClock(sim_epoch_iso=sim_now.isoformat(), real_epoch=real_now, speed=600)
    fake = FakeProvider([(201, True)])
    client = ProviderClient(BASE, httpx.AsyncClient(transport=httpx.MockTransport(fake.handler)))
    return DispatcherActivities(client, clock, load_campaign_config(), "http://cb.test/webhooks/telephony")


def test_activity_checks_window_with_real_clock():
    acts = _activities(datetime(2026, 11, 12, 12, 0, tzinfo=IST))  # Thursday noon
    assert acts.window_open_now("c_acme_renewals")
    acts = _activities(datetime(2026, 11, 12, 21, 30, tzinfo=IST))  # after 21:00
    assert not acts.window_open_now("c_acme_feedback")


def test_place_call_activity_outside_window_sends_nothing():
    acts = _activities(datetime(2026, 11, 14, 12, 0, tzinfo=IST))  # Saturday: zen is closed
    out = asyncio.run(acts.place_call(PlaceCallInput("r1", "c_zen_winback", "zen_0001", "+917401376156")))
    assert out.outcome == NOT_PLACED and out.reason == "window_closed"


def test_outcome_survives_temporal_serialization():
    """Activity results are stored in Temporal history as JSON and read back on
    replay. Make sure nothing is lost on the way."""
    conv = DataConverter.default.payload_converter
    original = PlaceCallOutcome(ACCEPTED, call=CallInfo.from_dict(ENDED))
    payload = conv.to_payloads([original])
    back = conv.from_payloads(payload, [PlaceCallOutcome])[0]
    assert back == original
    assert back.call.result.status == "completed"
