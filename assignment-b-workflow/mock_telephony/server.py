"""
Mock telephony provider for Assignment B.

Behaves like a (slightly hostile) real carrier API:
  * simulated clock running faster than real time
  * transient 503s, ambiguous 504s (the call MAY have been placed)
  * asynchronous result webhooks: duplicated sometimes, lost sometimes, retried on failure
  * enforces calling rules and records every violation in /admin/report

You should treat this server as a black box defined by the README's API section.
You are welcome to read the code, but do not modify it - we grade against the original.
"""
from __future__ import annotations

import asyncio
import csv
import hashlib
import hmac
import json
import os
import random
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

IST = timezone(timedelta(hours=5, minutes=30))
DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
TERMINAL = {"interested", "not_interested", "wrong_number", "dnd"}


# --------------------------------------------------------------------------- clock
class SimClock:
    def __init__(self, sim_start: datetime, speed: float):
        self.sim_start = sim_start
        self.speed = speed
        self.real_start = time.time()

    def now(self) -> datetime:
        return self.sim_start + timedelta(seconds=(time.time() - self.real_start) * self.speed)

    async def sleep_sim(self, sim_seconds: float):
        await asyncio.sleep(max(0.0, sim_seconds / self.speed))


def hm(s: str) -> tuple[int, int]:
    h, m = s.split(":")
    return int(h), int(m)


def in_window(t: datetime, start: str, end: str, days: list[str] | None, grace_s: float = 0) -> bool:
    """True if t is within [start, end) on an allowed day, tolerating `grace_s` either side."""
    for probe in (t, t - timedelta(seconds=grace_s), t + timedelta(seconds=grace_s)):
        lt = probe.astimezone(IST)
        if days is not None and DAYS[lt.weekday()] not in days:
            continue
        sh, sm = hm(start)
        eh, em = hm(end)
        s = lt.replace(hour=sh, minute=sm, second=0, microsecond=0)
        e = lt.replace(hour=eh, minute=em, second=0, microsecond=0)
        if s <= lt < e:
            return True
    return False


def next_open(t: datetime, start: str, end: str, days: list[str]) -> datetime:
    """Earliest instant >= t that is inside the window."""
    lt = t.astimezone(IST)
    for d in range(0, 9):
        day = (lt + timedelta(days=d)).replace(second=0, microsecond=0)
        if DAYS[day.weekday()] not in days:
            continue
        sh, sm = hm(start)
        eh, em = hm(end)
        s = day.replace(hour=sh, minute=sm)
        e = day.replace(hour=eh, minute=em)
        if d == 0:
            if lt < s:
                return s
            if lt < e:
                return lt
            continue
        return s
    return lt


# --------------------------------------------------------------------------- state
@dataclass
class Call:
    call_sid: str
    request_id: str
    tenant_id: str
    campaign_id: str
    lead_id: str
    phone: str
    callback_url: str
    attempt_no: int
    created_at: datetime
    planned_status: str
    planned_disposition: Optional[str]
    state: str = "active"  # active | ended
    status: Optional[str] = None
    disposition: Optional[str] = None
    callback_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    duration_sec: int = 0

    def result(self) -> dict:
        return {
            "event": "call.completed",
            "call_sid": self.call_sid,
            "request_id": self.request_id,
            "tenant_id": self.tenant_id,
            "campaign_id": self.campaign_id,
            "lead_id": self.lead_id,
            "phone": self.phone,
            "status": self.status,
            "disposition": self.disposition,
            "callback_at": self.callback_at.isoformat() if self.callback_at else None,
            "started_at": self.created_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_sec": self.duration_sec,
        }

    def public(self) -> dict:
        d = {"call_sid": self.call_sid, "request_id": self.request_id, "campaign_id": self.campaign_id,
             "lead_id": self.lead_id, "state": self.state, "created_at": self.created_at.isoformat()}
        if self.state == "ended":
            d["result"] = self.result()
        return d


@dataclass
class World:
    cfg: dict
    clock: SimClock
    seed: int
    failure_scale: float
    rng: random.Random
    tenants: dict = field(default_factory=dict)
    campaigns: dict = field(default_factory=dict)
    leads: dict = field(default_factory=dict)  # (campaign_id, lead_id) -> phone
    calls: dict = field(default_factory=dict)
    by_request: dict = field(default_factory=dict)
    active_tenant: Counter = field(default_factory=Counter)
    active_phone: dict = field(default_factory=dict)
    attempts: Counter = field(default_factory=Counter)
    last_end: dict = field(default_factory=dict)  # (cid, lid) -> Call
    terminal: dict = field(default_factory=dict)  # (cid, lid) -> disposition
    dnd_phones: dict = field(default_factory=dict)  # phone -> sim time of dnd
    violations: list = field(default_factory=list)
    callback_lateness: list = field(default_factory=list)
    webhook_stats: Counter = field(default_factory=Counter)
    max_seen_tenant: Counter = field(default_factory=Counter)


W: World  # set in create_app


def violation(kind: str, campaign_id: str, lead_id: str, detail: str):
    W.violations.append({"type": kind, "sim_time": W.clock.now().isoformat(),
                         "campaign_id": campaign_id, "lead_id": lead_id, "detail": detail})


def plan_outcome(campaign_id: str, lead_id: str, attempt_no: int) -> tuple[str, Optional[str]]:
    """Deterministic per (seed, campaign, lead, attempt) so runs are comparable."""
    h = hashlib.sha256(f"{W.seed}:{campaign_id}:{lead_id}".encode()).hexdigest()
    persona = int(h[:8], 16) / 0xFFFFFFFF
    r = random.Random(f"{W.seed}:{campaign_id}:{lead_id}:{attempt_no}")
    if persona < 0.10:  # unreachable
        return (r.choice(["no_answer", "no_answer", "busy"]), None)
    x = r.random()
    if x < 0.08:
        return ("failed", None)
    if x < 0.30:
        return ("no_answer", None)
    if x < 0.38:
        return ("busy", None)
    disp = r.choices(["interested", "not_interested", "callback_requested", "wrong_number", "dnd"],
                     [28, 22, 38, 6, 6])[0]
    return ("completed", disp)


# --------------------------------------------------------------------------- lifecycle
async def run_call(call: Call):
    r = random.Random(f"{W.seed}:life:{call.call_sid}")
    st = call.planned_status
    if st == "failed":
        await W.clock.sleep_sim(r.uniform(3, 10))
    elif st == "busy":
        await W.clock.sleep_sim(r.uniform(5, 12))
    elif st == "no_answer":
        await W.clock.sleep_sim(r.uniform(30, 45))
    else:
        await W.clock.sleep_sim(r.uniform(15, 30))
        call.started_at = W.clock.now()
        talk = r.uniform(60, 360)
        await W.clock.sleep_sim(talk)
        call.duration_sec = int(talk)

    now = W.clock.now()
    call.state = "ended"
    call.status = st
    call.ended_at = now
    call.disposition = call.planned_disposition
    if call.disposition == "callback_requested":
        call.callback_at = (now + timedelta(minutes=r.randint(30, 240))).replace(second=0, microsecond=0)
    key = (call.campaign_id, call.lead_id)
    W.last_end[key] = call
    if call.disposition in TERMINAL:
        W.terminal[key] = call.disposition
    if call.disposition == "dnd":
        W.dnd_phones.setdefault(call.phone, now)
    W.active_tenant[call.tenant_id] -= 1
    if W.active_phone.get(call.phone) == call.call_sid:
        del W.active_phone[call.phone]

    asyncio.create_task(deliver_webhook(call))


async def deliver_webhook(call: Call):
    r = random.Random(f"{W.seed}:wh:{call.call_sid}")
    if r.random() < 0.03 * W.failure_scale:
        W.webhook_stats["lost"] += 1
        return
    copies = 2 if r.random() < 0.10 * W.failure_scale else 1
    body = json.dumps(call.result()).encode()
    sig = hmac.new(W.cfg["webhook_secret"].encode(), body, hashlib.sha256).hexdigest()
    headers = {"Content-Type": "application/json", "X-AU-Signature": sig,
               "X-AU-Delivery-Id": str(uuid.uuid4())}
    async with httpx.AsyncClient(timeout=5.0) as client:
        for copy in range(copies):
            if copy:
                W.webhook_stats["duplicates"] += 1
                await asyncio.sleep(r.uniform(0.3, 1.5))
            for attempt, backoff in enumerate([0, 1, 2, 4]):
                if backoff:
                    await asyncio.sleep(backoff)
                try:
                    resp = await client.post(call.callback_url, content=body, headers=headers)
                    if 200 <= resp.status_code < 300:
                        W.webhook_stats["delivered"] += 1
                        break
                    W.webhook_stats["non_2xx"] += 1
                except httpx.HTTPError:
                    W.webhook_stats["connect_errors"] += 1
            else:
                W.webhook_stats["gave_up"] += 1


# --------------------------------------------------------------------------- app
def create_app(fixtures_path: str, speed: float, seed: int, failure_scale: float = 1.0) -> FastAPI:
    global W
    cfg = json.load(open(fixtures_path))
    sim_start = datetime.fromisoformat(cfg["sim_start"])
    W = World(cfg=cfg, clock=SimClock(sim_start, speed), seed=seed,
              failure_scale=failure_scale, rng=random.Random(seed))
    base = os.path.dirname(os.path.abspath(fixtures_path))
    for t in cfg["tenants"]:
        W.tenants[t["tenant_id"]] = t
    for c in cfg["campaigns"]:
        W.campaigns[c["campaign_id"]] = c
        with open(os.path.join(base, c["leads_file"])) as f:
            for row in csv.DictReader(f):
                W.leads[(c["campaign_id"], row["lead_id"])] = row["phone"]

    app = FastAPI(title="Agentic Universe - Mock Telephony Provider", version="1.0")

    @app.get("/v1/clock")
    async def clock():
        return {"sim_now": W.clock.now().isoformat(), "speed": W.clock.speed,
                "sim_epoch": W.clock.sim_start.isoformat(), "real_epoch": W.clock.real_start}

    @app.post("/v1/calls")
    async def create_call(req: Request):
        try:
            body = await req.json()
        except Exception:
            raise HTTPException(400, {"error": "invalid_json"})
        for k in ("request_id", "campaign_id", "lead_id", "phone", "callback_url"):
            if not body.get(k):
                raise HTTPException(400, {"error": "missing_field", "field": k})
        rid = body["request_id"]
        if rid in W.by_request:  # idempotent replay
            return JSONResponse(W.calls[W.by_request[rid]].public(), status_code=200)

        r = W.rng
        if r.random() < 0.05 * W.failure_scale:
            return JSONResponse({"error": "service_unavailable", "retryable": True}, status_code=503)
        if r.random() < 0.02 * W.failure_scale:
            await asyncio.sleep(2.0)
            return JSONResponse({"error": "upstream_timeout", "detail": "outcome unknown"}, status_code=504)

        cid, lid, phone = body["campaign_id"], body["lead_id"], body["phone"]
        camp = W.campaigns.get(cid)
        if not camp or W.leads.get((cid, lid)) != phone:
            raise HTTPException(422, {"error": "unknown_lead"})
        tenant = W.tenants[camp["tenant_id"]]
        now = W.clock.now()
        grace = W.cfg.get("window_grace_seconds", 120)
        key = (cid, lid)

        def reject(code: int, kind: str, detail: str):
            violation(kind, cid, lid, detail)
            return JSONResponse({"error": kind, "detail": detail}, status_code=code)

        rw = W.cfg["regulatory_window"]
        w = camp["window"]
        if not in_window(now, rw["start"], rw["end"], None, grace) or \
                not in_window(now, w["start"], w["end"], w["days"], grace):
            return reject(422, "outside_calling_window", f"sim_now={now.isoformat()}")
        if phone in W.dnd_phones:
            return reject(422, "dnd_violation", "phone opted out (dnd) earlier")
        if key in W.terminal:
            return reject(422, "dial_after_terminal", f"lead already {W.terminal[key]}")
        if W.attempts[key] >= camp["max_attempts"]:
            return reject(422, "max_attempts_exceeded", f"attempts={W.attempts[key]}")
        if phone in W.active_phone:
            return reject(409, "concurrent_dial_same_phone", f"active call {W.active_phone[phone]}")
        if W.active_tenant[tenant["tenant_id"]] >= tenant["max_concurrent_calls"]:
            return reject(429, "tenant_concurrency_exceeded",
                          f"active={W.active_tenant[tenant['tenant_id']]} max={tenant['max_concurrent_calls']}")
        last = W.last_end.get(key)
        if last is not None:
            rp = W.cfg["retry_policy"]
            if last.status in ("no_answer", "busy", "failed"):
                mins = rp[f"{last.status}_minutes"]
                earliest = last.ended_at + timedelta(minutes=mins)
                if now < earliest - timedelta(seconds=grace):
                    return reject(422, "retry_too_early",
                                  f"last={last.status} at {last.ended_at.isoformat()}, earliest={earliest.isoformat()}")
            if last.disposition == "callback_requested":
                if now < last.callback_at - timedelta(seconds=grace):
                    return reject(422, "callback_too_early", f"callback_at={last.callback_at.isoformat()}")
                due = next_open(last.callback_at, w["start"], w["end"], w["days"])
                W.callback_lateness.append(max(0.0, (now - due).total_seconds() / 60))

        # accept
        W.attempts[key] += 1
        st, disp = plan_outcome(cid, lid, W.attempts[key])
        call = Call(call_sid="CA" + uuid.uuid4().hex[:24], request_id=rid, tenant_id=tenant["tenant_id"],
                    campaign_id=cid, lead_id=lid, phone=phone, callback_url=body["callback_url"],
                    attempt_no=W.attempts[key], created_at=now, planned_status=st, planned_disposition=disp)
        W.calls[call.call_sid] = call
        W.by_request[rid] = call.call_sid
        W.active_tenant[tenant["tenant_id"]] += 1
        W.max_seen_tenant[tenant["tenant_id"]] = max(W.max_seen_tenant[tenant["tenant_id"]],
                                                     W.active_tenant[tenant["tenant_id"]])
        W.active_phone[phone] = call.call_sid
        asyncio.create_task(run_call(call))

        if r.random() < 0.04 * W.failure_scale:  # placed, but the response is lost
            await asyncio.sleep(2.0)
            return JSONResponse({"error": "upstream_timeout", "detail": "outcome unknown"}, status_code=504)
        return JSONResponse(call.public(), status_code=201)

    @app.get("/v1/calls/{call_sid}")
    async def get_call(call_sid: str):
        c = W.calls.get(call_sid)
        if not c:
            raise HTTPException(404, {"error": "not_found"})
        return c.public()

    @app.get("/v1/calls")
    async def find_call(request_id: str):
        sid = W.by_request.get(request_id)
        if not sid:
            raise HTTPException(404, {"error": "not_found"})
        return W.calls[sid].public()

    # ------------------------------------------------------------------ admin (grading)
    @app.get("/admin/report")
    async def report():
        lat = sorted(W.callback_lateness)

        def pct(p):
            return round(lat[min(len(lat) - 1, int(p * len(lat)))], 1) if lat else None
        return {
            "sim_now": W.clock.now().isoformat(),
            "calls_total": len(W.calls),
            "calls_active": sum(1 for c in W.calls.values() if c.state == "active"),
            "calls_by_status": dict(Counter(c.status for c in W.calls.values() if c.state == "ended")),
            "violation_count": len(W.violations),
            "violations_by_type": dict(Counter(v["type"] for v in W.violations)),
            "violations": W.violations[-200:],
            "max_concurrency_seen": dict(W.max_seen_tenant),
            "callback_lateness_minutes": {"count": len(lat), "p50": pct(0.5), "p90": pct(0.9),
                                          "max": round(lat[-1], 1) if lat else None},
            "webhooks": dict(W.webhook_stats),
        }

    @app.get("/admin/leads")
    async def leads():
        """Expected final state of every lead, derived from the provider's own records."""
        out = []
        for (cid, lid), phone in sorted(W.leads.items()):
            key = (cid, lid)
            last = W.last_end.get(key)
            camp = W.campaigns[cid]
            busy = any(c.state == "active" for c in W.calls.values() if (c.campaign_id, c.lead_id) == key)
            # precedence: completed > exhausted > dnd_blocked
            if key in W.terminal:
                state = "completed"
            elif W.attempts[key] >= camp["max_attempts"] and not busy:
                state = "exhausted"
            elif phone in W.dnd_phones and not busy:
                state = "dnd_blocked"
            else:
                state = "incomplete"
            out.append({"campaign_id": cid, "lead_id": lid, "expected_state": state,
                        "attempts": W.attempts[key],
                        "last_status": last.status if last else "",
                        "last_disposition": (last.disposition or "") if last else ""})
        return out

    return app
