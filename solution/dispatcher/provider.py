"""Client for the telephony provider's API.

Turns every response from POST /v1/calls into one of three clear answers:

  accepted    the provider placed the call (we have its call_sid)
  not_placed  we know for sure no call was placed
  rejected    the provider says we broke a rule (429 / 409 / 422)

The key to safety is the request_id. It is chosen by the workflow before any request
is sent, and every retry uses the same one. The provider places at most one call per
request_id, so retrying after a 503, a 504 or a crash can never place a second call.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable

import httpx

log = logging.getLogger(__name__)

ACCEPTED = "accepted"
NOT_PLACED = "not_placed"
REJECTED = "rejected"

# Real seconds to wait for an answer to POST /v1/calls. A normal answer takes a few
# milliseconds. A request that hangs is abandoned and treated like a 504: look the
# call up by request_id, then retry with the same request_id. Waiting it out instead
# costs a lot at simulated speed (2 real seconds is 20 simulated minutes at 600x),
# and the lead holds a call slot the whole time.
DEFAULT_POST_TIMEOUT = 0.5

# Real seconds to wait between retries after a 503 or an unknown outcome. Kept short:
# at 600x speed, 1.5 real seconds in total is already about 15 simulated minutes,
# and the lead holds one of its tenant's call slots while we retry.
DEFAULT_BACKOFF = (0.1, 0.2, 0.4, 0.8)


@dataclass
class CallResult:
    """The outcome of an ended call: the webhook body, or `result` from GET /v1/calls.

    Times are kept as ISO strings, exactly as the provider sent them."""

    call_sid: str
    request_id: str
    campaign_id: str
    lead_id: str
    phone: str
    status: str  # completed | no_answer | busy | failed
    disposition: str | None
    callback_at: str | None
    ended_at: str

    @staticmethod
    def from_dict(d: dict) -> "CallResult":
        return CallResult(
            call_sid=d["call_sid"],
            request_id=d["request_id"],
            campaign_id=d["campaign_id"],
            lead_id=d["lead_id"],
            phone=d["phone"],
            status=d["status"],
            disposition=d.get("disposition"),
            callback_at=d.get("callback_at"),
            ended_at=d["ended_at"],
        )


@dataclass
class CallInfo:
    """What the provider knows about one call right now."""

    call_sid: str
    request_id: str
    state: str  # active | ended
    created_at: str
    result: CallResult | None = None

    @staticmethod
    def from_dict(d: dict) -> "CallInfo":
        return CallInfo(
            call_sid=d["call_sid"],
            request_id=d["request_id"],
            state=d["state"],
            created_at=d["created_at"],
            result=CallResult.from_dict(d["result"]) if d.get("result") else None,
        )


@dataclass
class PlaceCallOutcome:
    outcome: str  # accepted | not_placed | rejected
    call: CallInfo | None = None  # set when accepted
    reason: str = ""  # why it was not placed or rejected


class ProviderClient:
    def __init__(self, base_url: str, client: httpx.AsyncClient | None = None,
                 timeout: float = 5.0, post_timeout: float = DEFAULT_POST_TIMEOUT):
        self.base_url = base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self.post_timeout = post_timeout

    async def aclose(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------------ lookups
    async def get_call(self, call_sid: str) -> CallInfo | None:
        """GET /v1/calls/{call_sid}. None if the provider has no such call."""
        resp = await self._client.get(f"{self.base_url}/v1/calls/{call_sid}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return CallInfo.from_dict(resp.json())

    async def find_by_request_id(self, request_id: str) -> CallInfo | None:
        """GET /v1/calls?request_id=... None if no call was placed with that id."""
        resp = await self._client.get(f"{self.base_url}/v1/calls",
                                      params={"request_id": request_id})
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return CallInfo.from_dict(resp.json())

    # ------------------------------------------------------------------ placing
    async def place_call(
        self,
        *,
        request_id: str,
        campaign_id: str,
        lead_id: str,
        phone: str,
        callback_url: str,
        may_send: Callable[[], bool] = lambda: True,
        on_step: Callable[[], None] = lambda: None,
        backoff: tuple[float, ...] = DEFAULT_BACKOFF,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> PlaceCallOutcome:
        """Place one call, retrying safely with the same request_id.

        may_send is checked right before every POST (including retries). If it says
        no (for example the calling window has just closed), nothing is sent and the
        answer is not_placed.

        on_step is called before every HTTP request. The activity uses it to send a
        heartbeat to Temporal, so a killed worker is noticed within seconds.
        """
        body = {
            "request_id": request_id,
            "campaign_id": campaign_id,
            "lead_id": lead_id,
            "phone": phone,
            "callback_url": callback_url,
        }
        tries = len(backoff) + 1
        last_reason = ""
        for attempt in range(tries):
            if attempt:
                await sleep(backoff[attempt - 1])

            if not may_send():
                # Before saying "not placed", make sure an earlier unknown attempt
                # did not in fact place the call.
                if last_reason.startswith("unknown"):
                    found = await self._lookup(request_id, on_step)
                    if found is not None:
                        return PlaceCallOutcome(ACCEPTED, call=found)
                return PlaceCallOutcome(NOT_PLACED, reason="window_closed")

            try:
                on_step()
                resp = await self._client.post(f"{self.base_url}/v1/calls", json=body,
                                               timeout=self.post_timeout)
            except httpx.HTTPError as exc:
                # The request may or may not have reached the provider.
                last_reason = f"unknown:{type(exc).__name__}"
                log.warning("place_call %s: %s, checking whether the call was placed",
                            request_id, type(exc).__name__)
                found = await self._lookup(request_id, on_step)
                if found is not None:
                    return PlaceCallOutcome(ACCEPTED, call=found)
                continue

            code = resp.status_code
            if code in (200, 201):
                return PlaceCallOutcome(ACCEPTED, call=CallInfo.from_dict(resp.json()))
            if code == 503:
                last_reason = "unavailable"
                continue
            if code == 504:
                # Outcome unknown: ask the provider whether the call exists.
                last_reason = "unknown:504"
                found = await self._lookup(request_id, on_step)
                if found is not None:
                    return PlaceCallOutcome(ACCEPTED, call=found)
                continue
            if code in (409, 422, 429):
                detail = _error_detail(resp)
                log.error("place_call %s REJECTED %s: %s", request_id, code, detail)
                return PlaceCallOutcome(REJECTED, reason=f"{code}:{detail}")
            # Anything else (400, 500...) is unexpected: treat as unknown and check.
            last_reason = f"unknown:{code}"
            found = await self._lookup(request_id, on_step)
            if found is not None:
                return PlaceCallOutcome(ACCEPTED, call=found)

        # Out of retries. If any attempt had an unknown outcome, check once more so
        # a call that was in fact placed is never forgotten. If this lookup itself
        # fails, the error propagates and Temporal retries the whole activity with
        # the same request_id, which is safe.
        if last_reason.startswith("unknown"):
            found = await self._lookup(request_id, on_step)
            if found is not None:
                return PlaceCallOutcome(ACCEPTED, call=found)
        return PlaceCallOutcome(NOT_PLACED, reason=last_reason or "gave_up")


    async def _lookup(self, request_id: str, on_step: Callable[[], None]) -> CallInfo | None:
        on_step()
        return await self.find_by_request_id(request_id)


def _error_detail(resp: httpx.Response) -> str:
    try:
        body = resp.json()
    except ValueError:
        return resp.text[:200]
    detail = body.get("detail", body)
    if isinstance(detail, dict):
        return str(detail.get("error", detail))
    return str(detail)
