"""Tests for the runner's handling of Temporal outages."""
import asyncio

import pytest
from temporalio.service import RPCError, RPCStatusCode

from dispatcher.main import wait_for_result


class FlakyHandle:
    def __init__(self, errors):
        self.errors = list(errors)
        self.calls = 0

    async def result(self):
        self.calls += 1
        if self.errors:
            raise self.errors.pop(0)
        return "done"


async def _running_task():
    return asyncio.create_task(asyncio.sleep(10))


def test_waits_through_a_temporal_outage():
    async def run():
        worker = await _running_task()
        handle = FlakyHandle([RPCError("transport error", RPCStatusCode.UNKNOWN, b""),
                              RPCError("unavailable", RPCStatusCode.UNAVAILABLE, b"")])
        try:
            return await wait_for_result(handle, worker, retry_seconds=0), handle.calls
        finally:
            worker.cancel()

    assert asyncio.run(run()) == ("done", 3)


def test_other_errors_are_not_retried():
    async def run():
        worker = await _running_task()
        handle = FlakyHandle([RPCError("nope", RPCStatusCode.PERMISSION_DENIED, b"")])
        try:
            await wait_for_result(handle, worker, retry_seconds=0)
        finally:
            worker.cancel()

    with pytest.raises(RPCError):
        asyncio.run(run())


# --------------------------------------------------------------------------- stale runs
import dataclasses
import uuid
from datetime import datetime, timezone

import httpx
from temporalio.client import WorkflowExecutionStatus
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment

from dispatcher.activities import DispatcherActivities, PlaceCallInput
from dispatcher.clock import IST, SimClock
from dispatcher.config import load_campaign_config, load_settings
from dispatcher.main import close_stale_runs
from dispatcher.provider import ProviderClient
from dispatcher.workflow import DispatchInput, DispatcherWorkflow


def _activities(expected: str, posts: list) -> DispatcherActivities:
    def handler(request):
        posts.append(request)
        return httpx.Response(500)

    clock = SimClock(datetime(2026, 11, 12, 12, 0, tzinfo=IST).isoformat(),
                     datetime.now(timezone.utc).timestamp(), 600)
    client = ProviderClient("http://p.test", httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    return DispatcherActivities(client, clock, load_campaign_config(), "http://cb.test",
                                expected_workflow_id=expected)


def test_activity_refuses_a_workflow_from_another_provider_run():
    posts = []
    acts = _activities("campaign-dispatch-200", posts)
    env = ActivityEnvironment()
    env.info = dataclasses.replace(env.info, workflow_id="campaign-dispatch-100")
    inp = PlaceCallInput("c_acme_renewals:ren_0001:d1", "c_acme_renewals", "ren_0001", "+919000000001")
    with pytest.raises(ApplicationError) as err:
        asyncio.run(env.run(acts.place_call, inp))
    assert err.value.non_retryable and err.value.type == "StaleDispatchRun"
    with pytest.raises(ApplicationError):
        asyncio.run(env.run(acts.get_call, "CA1"))
    assert posts == []  # the provider was never contacted


def test_stale_runs_are_terminated_and_the_current_one_is_kept():
    async def run():
        try:
            env = await WorkflowEnvironment.start_local()
        except Exception as exc:  # pragma: no cover - depends on the machine
            pytest.skip(f"Temporal dev server unavailable: {exc}")
        async with env:
            settings = dataclasses.replace(load_settings(), task_queue=f"tq-{uuid.uuid4().hex[:8]}")
            clock = SimClock("2026-11-12T17:30:00+05:30", 1.0, 600)
            inp = DispatchInput(clock=clock, cfg=dataclasses.replace(load_campaign_config(), webhook_secret=""))
            # No worker on this task queue: both workflows just sit there "running".
            for wid in ("campaign-dispatch-100", "campaign-dispatch-200"):
                await env.client.start_workflow(DispatcherWorkflow.run, inp, id=wid,
                                                task_queue=settings.task_queue)
            closed = []
            for _ in range(50):  # the search index can take a moment to catch up
                closed += await close_stale_runs(env.client, settings, "campaign-dispatch-200")
                if closed:
                    break
                await asyncio.sleep(0.2)
            old = await env.client.get_workflow_handle("campaign-dispatch-100").describe()
            cur = await env.client.get_workflow_handle("campaign-dispatch-200").describe()
            return closed, old.status, cur.status

    closed, old, cur = asyncio.run(run())
    assert closed == ["campaign-dispatch-100"]
    assert old == WorkflowExecutionStatus.TERMINATED
    assert cur == WorkflowExecutionStatus.RUNNING
