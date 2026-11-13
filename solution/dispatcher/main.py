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
from temporalio.worker import Worker

from .activities import DispatcherActivities
from .clock import SimClock, fetch_clock
from .config import CampaignConfig, Settings, load_campaign_config, load_settings
from .provider import CallResult, ProviderClient
from .output import write_final_state
from .webhooks import create_app, status_to_dict
from .workflow import DispatchInput, DispatchResult, DispatcherWorkflow

log = logging.getLogger("dispatcher")


def workflow_id_for(clock: SimClock) -> str:
    """One workflow per provider run: the provider's clock start time identifies it."""
    return f"campaign-dispatch-{int(clock.real_epoch)}"


async def connect_temporal(settings: Settings) -> Client:
    if settings.uses_temporal_cloud:
        log.info("connecting to Temporal Cloud at %s", settings.temporal_address)
        return await Client.connect(settings.temporal_address,
                                    namespace=settings.temporal_namespace,
                                    api_key=settings.temporal_api_key, tls=True)
    log.info("connecting to Temporal at %s", settings.temporal_address)
    return await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)


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

    # 2. worker
    provider = ProviderClient(settings.provider_url, httpx.AsyncClient(timeout=5.0))
    activities = DispatcherActivities(provider, clock, cfg, settings.callback_url)
    worker = Worker(client, task_queue=settings.task_queue, workflows=[DispatcherWorkflow],
                    activities=[activities.place_call, activities.get_call])
    worker_task = asyncio.create_task(worker.run())

    # 3. workflow
    try:
        handle = await start_or_attach(client, settings, clock, cfg)
        result: DispatchResult = await handle.result()
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
