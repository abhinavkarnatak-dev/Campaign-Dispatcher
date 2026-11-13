"""Runs the whole dispatcher in one process: webhook receiver, Temporal worker, and the
campaign workflow.

    python -m dispatcher run

Start order matters: the webhook receiver is listening before the workflow can place
its first call, so no result is ever refused.

Restarting after a crash is safe. The workflow ID contains the provider's clock start
time, so:
  * restart after kill -9 (same provider run)   -> same ID, re-attach to the workflow
  * a brand new provider run                     -> new ID, a fresh workflow
  * restart after the workflow already finished  -> same ID, just read its result
"""
from __future__ import annotations

import asyncio
import logging
import signal
from dataclasses import replace

import httpx
import uvicorn
from temporalio.client import Client, WorkflowHandle
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker

from .activities import DispatcherActivities
from .clock import SimClock, fetch_clock, workflow_id_for
from .config import CampaignConfig, Settings, load_campaign_config, load_settings
from .provider import CallResult, ProviderClient
from .output import write_final_state
from .webhooks import create_app, status_to_dict
from .workflow import DispatchInput, DispatchResult, DispatcherWorkflow

log = logging.getLogger("dispatcher")


# Errors that mean "Temporal is briefly unreachable", for example while the server
# restarts. Waiting for the result is retried on these; anything else stops.
TRANSIENT_RPC = {RPCStatusCode.UNAVAILABLE, RPCStatusCode.UNKNOWN,
                 RPCStatusCode.DEADLINE_EXCEEDED}
RESULT_RETRY_SECONDS = 2.0


async def wait_for_result(handle: WorkflowHandle, worker_task: asyncio.Task,
                          retry_seconds: float = RESULT_RETRY_SECONDS) -> DispatchResult:
    """Wait for the workflow to finish, riding out short Temporal outages.

    The workflow's state lives in Temporal, so a server restart loses nothing; this
    process only has to keep waiting instead of exiting."""
    while True:
        try:
            return await handle.result()
        except RPCError as exc:
            if exc.status not in TRANSIENT_RPC or worker_task.done():
                raise
            log.warning("lost contact with Temporal (%s), retrying in %.0fs",
                        exc.message, retry_seconds)
            await asyncio.sleep(retry_seconds)


async def connect_temporal(settings: Settings) -> Client:
    if settings.uses_temporal_cloud:
        log.info("connecting to Temporal Cloud at %s", settings.temporal_address)
        return await Client.connect(settings.temporal_address,
                                    namespace=settings.temporal_namespace,
                                    api_key=settings.temporal_api_key, tls=True)
    log.info("connecting to Temporal at %s", settings.temporal_address)
    return await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)


async def close_stale_runs(client: Client, settings: Settings, workflow_id: str) -> list[str]:
    """Terminate dispatcher workflows left running on our task queue by an earlier
    provider run.

    Each provider run gets its own workflow. A workflow from an older run (for
    example one that was stopped partway) would otherwise be picked up by this
    worker and place calls against the new provider using the old run's state.
    The activities refuse to act for any workflow but the current one as well; this
    just closes such runs cleanly, before the worker starts."""
    query = (f'WorkflowType="CampaignDispatcher" AND ExecutionStatus="Running" '
             f'AND TaskQueue="{settings.task_queue}"')
    closed = []
    try:
        async for wf in client.list_workflows(query):
            if wf.id == workflow_id:
                continue
            await client.get_workflow_handle(wf.id, run_id=wf.run_id).terminate(
                reason=f"stale: belongs to an earlier provider run, now running {workflow_id}")
            closed.append(wf.id)
            log.warning("terminated stale workflow %s from an earlier provider run", wf.id)
    except RPCError as exc:  # e.g. search not available: the activity check still protects
        log.warning("could not look for stale workflows (%s); continuing", exc.message)
    return closed


async def start_or_attach(client: Client, settings: Settings, clock: SimClock,
                          cfg: CampaignConfig) -> WorkflowHandle:
    workflow_id = workflow_id_for(clock)
    try:
        handle = await client.start_workflow(
            DispatcherWorkflow.run,
            # The webhook secret is left out: workflow inputs are stored in history.
            DispatchInput(clock=clock, cfg=replace(cfg, webhook_secret="")),
            id=workflow_id,
            task_queue=settings.task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
        log.info("workflow %s running (started now, or re-attached after a restart)", workflow_id)
    except WorkflowAlreadyStartedError:
        # Typed handle, so the stored result is read back as a DispatchResult.
        handle = client.get_workflow_handle_for(DispatcherWorkflow.run, workflow_id)
        log.info("workflow %s already finished, reading its result", workflow_id)
    return handle


async def run(settings: Settings | None = None) -> DispatchResult:
    settings = settings or load_settings()
    cfg = load_campaign_config(settings.fixtures_dir)
    client = await connect_temporal(settings)
    clock = fetch_clock(settings.provider_url)
    workflow_id = workflow_id_for(clock)
    log.info("provider clock: sim start %s, speed %.0fx, %d leads in %d campaigns",
             clock.sim_epoch_iso, clock.speed, len(cfg.leads), len(cfg.campaigns))

    async def send_signal(result: CallResult) -> None:
        await client.get_workflow_handle(workflow_id).signal("call_result", result)

    async def get_status() -> dict:
        status = await client.get_workflow_handle(workflow_id).query(DispatcherWorkflow.status)
        return status_to_dict(status)

    # 1. webhook receiver
    app = create_app(cfg.webhook_secret, send_signal, get_status)
    server = uvicorn.Server(uvicorn.Config(app, host=settings.webhook_host,
                                           port=settings.webhook_port, log_level="warning"))
    server_task = asyncio.create_task(server.serve())
    while not server.started:
        if server_task.done():
            server_task.result()  # surfaces e.g. "port already in use"
        await asyncio.sleep(0.05)
    log.info("webhook receiver listening on %s", settings.callback_url)

    # 2. worker (after closing any workflow left over from an earlier provider run)
    await close_stale_runs(client, settings, workflow_id)
    provider = ProviderClient(settings.provider_url, httpx.AsyncClient(timeout=5.0))
    activities = DispatcherActivities(provider, clock, cfg, settings.callback_url,
                                      expected_workflow_id=workflow_id)
    worker = Worker(client, task_queue=settings.task_queue, workflows=[DispatcherWorkflow],
                    activities=[activities.place_call, activities.get_call])
    worker_task = asyncio.create_task(worker.run())

    # 3. workflow
    try:
        handle = await start_or_attach(client, settings, clock, cfg)
        result = await wait_for_result(handle, worker_task)
    finally:
        await worker.shutdown()
        await asyncio.gather(worker_task, return_exceptions=True)
        server.should_exit = True
        await asyncio.gather(server_task, return_exceptions=True)
        await provider.aclose()

    counts: dict[str, int] = {}
    for lead in result.leads:
        counts[lead.final_state] = counts.get(lead.final_state, 0) + 1
    log.info("all %d leads finished: %s", len(result.leads), counts)
    log.info("workflow counters: %s", result.counters)
    path = write_final_state(result.leads, settings.output_path)
    log.info("wrote %s", path)
    return result


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("temporalio").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    loop = asyncio.new_event_loop()
    task = loop.create_task(run())
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, task.cancel)
        except (NotImplementedError, RuntimeError):  # Windows: Ctrl+C raises KeyboardInterrupt
            pass
    try:
        loop.run_until_complete(task)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("stopped. The workflow keeps its state in Temporal; run again to continue.")
    finally:
        loop.close()
