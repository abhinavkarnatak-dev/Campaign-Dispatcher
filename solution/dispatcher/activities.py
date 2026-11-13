"""Temporal activities: the only code that talks to the provider.

The workflow never touches the network, because Temporal replays it after a crash.
Everything external happens here, and Temporal saves each activity's result in the
workflow history, so a replay reuses the saved answer instead of calling again.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from temporalio import activity
from temporalio.exceptions import ApplicationError

from .clock import SimClock
from .config import CampaignConfig
from .provider import CallInfo, PlaceCallOutcome, ProviderClient
from .scheduling import can_start_call, close_margin_for


def _heartbeat() -> None:
    """Tell Temporal this activity is still alive. If the worker is killed, the
    missing heartbeats let Temporal retry the activity within seconds."""
    if activity.in_activity():
        activity.heartbeat()


@dataclass
class PlaceCallInput:
    request_id: str  # chosen by the workflow before this activity runs
    campaign_id: str
    lead_id: str
    phone: str


class DispatcherActivities:
    """Activities share one HTTP client, the clock and the campaign config, which
    are set up once when the worker starts."""

    def __init__(self, provider: ProviderClient, clock: SimClock, cfg: CampaignConfig,
                 callback_url: str, expected_workflow_id: str | None = None):
        self.provider = provider
        self.clock = clock
        self.cfg = cfg
        self.callback_url = callback_url
        self.expected_workflow_id = expected_workflow_id

    def check_caller(self) -> None:
        """Refuse to act for any workflow but the one for the current provider run.

        A workflow left over from an earlier provider run must never place calls on
        this provider with its own, unrelated state. Non-retryable: retrying cannot
        make it the right workflow."""
        if self.expected_workflow_id is None or not activity.in_activity():
            return
        caller = activity.info().workflow_id
        if caller != self.expected_workflow_id:
            raise ApplicationError(
                f"workflow {caller} is not the current provider run "
                f"({self.expected_workflow_id}); refusing to contact the provider",
                type="StaleDispatchRun", non_retryable=True)

    def window_open_now(self, campaign_id: str) -> bool:
        """R1, checked against the real current time at the moment of sending.

        Unlike the workflow, an activity is never replayed, so reading the machine
        clock here is fine, and it is the most accurate moment to check."""
        sim_now = self.clock.sim_at(datetime.now(timezone.utc))
        return can_start_call(sim_now, self.cfg.campaign(campaign_id), self.cfg.regulatory_window,
                              close_margin_for(self.clock.speed))

    @activity.defn(name="place_call")
    async def place_call(self, inp: PlaceCallInput) -> PlaceCallOutcome:
        self.check_caller()
        return await self.provider.place_call(
            request_id=inp.request_id,
            campaign_id=inp.campaign_id,
            lead_id=inp.lead_id,
            phone=inp.phone,
            callback_url=self.callback_url,
            may_send=lambda: self.window_open_now(inp.campaign_id),
            on_step=_heartbeat,
        )

    @activity.defn(name="get_call")
    async def get_call(self, call_sid: str) -> CallInfo | None:
        self.check_caller()
        return await self.provider.get_call(call_sid)
