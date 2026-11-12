# Assignment B: Durable Campaign Dispatcher (Workflow)

## Context

Agentic Universe calls thousands of customers a day on behalf of insurers and consumer brands.
A **campaign** is a list of leads that must be called within a **calling window**, under a
per-tenant **concurrency limit**, with **retries** when nobody picks up and **callbacks** when
the customer asks us to call later.

It sounds simple, but production is where it breaks. Carriers time out. Webhooks arrive twice,
or never. Workers crash mid-dial. A bug here means calling someone at 10 pm, calling the same
person twice at once, or calling a customer who said "do not call me". In a regulated industry,
each of those is a complaint, a fine, or a lost client.

Your job is to build the **dispatcher**: the workflow service that decides who to call and when,
places calls through a telephony provider, processes results, and keeps going no matter what
fails.

Read [`../docs/GLOSSARY.md`](../docs/GLOSSARY.md) first.

## What you're given

```
assignment-b-workflow/
├── fixtures/
│   ├── campaigns.json            # tenants, campaigns, windows, retry policy, webhook secret
│   └── leads_<campaign_id>.csv   # 200 leads across 3 campaigns
└── mock_telephony/               # a mock carrier API (FastAPI). Do not modify it.
```

Run the mock provider:

```bash
pip install -r requirements.txt
cd assignment-b-workflow
python -m mock_telephony                      # http://127.0.0.1:8099, docs at /docs
python -m mock_telephony --failure-scale 0    # no injected failures, handy while developing
python -m mock_telephony --speed 1200         # faster simulated clock
```

The mock has a **simulated clock**. By default it runs at 600× real time (1 real second = 10
simulated minutes) and starts at **Thu 12 Nov 2026, 17:30 IST**. A full run takes a few real
minutes. **All business rules use the provider's simulated time, never your machine's clock.**

## What to build

A service (one process or several, your choice) that:

1. Loads the campaigns and leads from `fixtures/`.
2. **Dispatches calls** through the provider's API while obeying every rule below.
3. **Receives result webhooks** from the provider and decides what happens to each lead next.
4. Runs until **every lead reaches a final state**, then writes `final_state.csv` (contract
   below).
5. **Survives crashes.** We will `kill -9` your process at random points and start it again.
   It must resume without losing a lead, double-dialling, or breaking any rule.

You may use **Temporal** (`temporalio` Python SDK, via `temporal server start-dev`), or build
your own on a database-backed state machine or job queue (SQLite or Postgres). Both are fine.
We care that you understand *why* your design survives failure, not which tool you picked.
Explain the choice in `SUBMISSION.md`.

## Business rules

| # | Rule |
|---|---|
| R1 | **Calling window:** a call may only be **started** inside the campaign's `window` (IST, `[start, end)`, on the listed `days`) **and** inside the regulatory window 09:00–21:00 IST. A call already in progress at window close may finish. The provider allows 120 simulated seconds of clock skew. |
| R2 | **Concurrency:** a tenant may never have more active calls than `max_concurrent_calls`. The limit is **shared across all of that tenant's campaigns**. |
| R3 | **One call per phone:** never have two active calls to the same phone number. Some phones appear in two campaigns. |
| R4 | **Attempts:** at most `max_attempts` (3) calls per lead per campaign. An *attempt* is a call the provider **accepted**. Requests rejected with 503/429, or never placed, don't count. |
| R5 | **Retries:** after `no_answer` or `busy`, wait `60` simulated minutes before the next attempt. After `failed`, wait `15`. Measure from the call's `ended_at`. |
| R6 | **Callbacks:** a `callback_requested` disposition comes with `callback_at`. Don't call before it. Call as soon as possible after it, and if it falls outside the window, at the next window opening. **Due callbacks go before retries, and retries before fresh leads.** |
| R7 | **Terminal dispositions:** `interested`, `not_interested`, `wrong_number` and `dnd` end the lead. Never dial it again. |
| R8 | **DND is global:** after `dnd`, never call **that phone number** again in **any** campaign. Other leads with that phone that haven't been completed or exhausted become `dnd_blocked`. |
| R9 | **No lost leads:** every lead must end as `completed`, `exhausted` (used all attempts without a terminal disposition), or `dnd_blocked`. **Precedence: `completed` > `exhausted` > `dnd_blocked`.** |

## Provider API

Base URL `http://127.0.0.1:8099`. Interactive docs at `/docs`.

### `GET /v1/clock`
```json
{"sim_now": "2026-11-12T17:45:26+05:30", "speed": 600.0,
 "sim_epoch": "2026-11-12T17:30:00+05:30", "real_epoch": 1794397432.60}
```
`sim_now = sim_epoch + (time.time() - real_epoch) * speed`. You can compute it locally instead
of polling.

### `POST /v1/calls`: place a call
```json
{"request_id": "<your idempotency key>", "campaign_id": "c_acme_renewals",
 "lead_id": "ren_0001", "phone": "+919876543210",
 "callback_url": "http://127.0.0.1:8200/webhooks/telephony"}
```

| Response | Meaning |
|---|---|
| `201` | Accepted. Body includes `call_sid`. |
| `200` | A call with this `request_id` already exists; returns it. **Retrying with the same `request_id` is always safe.** |
| `503` | Transient; call **not** placed. Retry later. |
| `504` | **Outcome unknown**: the call may or may not have been placed. |
| `429` / `409` / `422` | A rule was broken (concurrency, same phone, window, attempts, DND, too early...). **Every one of these is recorded as a violation.** |

### `GET /v1/calls/{call_sid}` and `GET /v1/calls?request_id=...`
Return `{"call_sid", "request_id", "state": "active"|"ended", ..., "result": {...}}`. `result`
is present once ended and has the same shape as the webhook body. Returns `404` if no such call
exists.

### Result webhook (provider → you)
`POST <callback_url>` when a call ends:
```json
{"event": "call.completed", "call_sid": "CA...", "request_id": "...", "tenant_id": "acme_insurance",
 "campaign_id": "c_acme_renewals", "lead_id": "ren_0001", "phone": "+919876543210",
 "status": "completed", "disposition": "callback_requested",
 "callback_at": "2026-11-12T20:15:00+05:30",
 "started_at": "...", "ended_at": "...", "duration_sec": 142}
```
- `status` is `completed` (the customer answered; `disposition` is set), `no_answer`, `busy`
  or `failed` (`disposition` is null).
- Header `X-AU-Signature` = hex HMAC-SHA256 of the raw body, keyed with `webhook_secret` from
  `campaigns.json`.
- Reply 2xx. Anything else, or a connection error, triggers up to 3 redeliveries.

### Things the provider will do to you
- Return 503s and **ambiguous 504s**.
- Deliver some webhooks **twice**.
- **Never deliver** some webhooks at all. Your service has to notice and recover.
- Deliver a webhook **before** you have read the 201 response for that call.

### Admin endpoints (for your own checking; we use them to grade)
- `GET /admin/report`: violations by type, max concurrency seen, callback lateness, webhook
  stats.
- `GET /admin/leads`: the provider's view of each lead's expected final state.

**Target: `violation_count: 0`, and your `final_state.csv` matches `/admin/leads`.**

## Output contract

`final_state.csv`, sorted by `campaign_id, lead_id`:
```
campaign_id,lead_id,final_state,attempts,last_status,last_disposition
c_acme_feedback,fb_0001,completed,2,completed,interested
c_acme_feedback,fb_0002,exhausted,3,no_answer,
```
- `final_state`: `completed` | `exhausted` | `dnd_blocked`
- `attempts`: calls accepted by the provider for this lead
- `last_status` / `last_disposition`: from the lead's most recent ended call. Empty if none.

## Scope

**Core (expected within the time box):**
- R1–R9 enforced, with 0 violations on a clean run
- Idempotent webhook handling (duplicates are harmless) and signature verification
- Correct handling of 503 and ambiguous 504
- Recovery from lost webhooks
- Survives `kill -9` and restart mid-run without violations or lost leads
- `final_state.csv` written and matching the provider
- Tests for the scheduling logic: window maths, next eligible time, priority ordering. Use a
  fake clock; don't make tests wait on the mock.
- `SUBMISSION.md` (with a **state diagram** of a lead's lifecycle; ASCII or Mermaid is fine)
  and `AI_USAGE.md`

**Stretch (pick any; don't do them all):**
- Implement it on Temporal and explain what Temporal gives you versus a DB state machine
- Horizontal scale: two dispatcher processes running at once, still with 0 violations
- Observability: structured logs, or a `/status` endpoint with per-campaign progress
- Minimise callback lateness (see `callback_lateness_minutes` in `/admin/report`)
- A short note on what changes at 1M leads/day across 50 tenants

## How we evaluate

1. **Correctness under failure:** we start the mock on an **unseen seed**, run your service, and
   `kill -9` it a few times. We check the violation count and diff your output against the
   provider's.
2. **Design:** your state model, how you guarantee at-most-once dialling, and how you handle
   ambiguity (504s, lost webhooks, crashes between "decided to dial" and "dialled").
3. **Code quality and tests:** clear, structured, with the scheduling rules tested.
4. **Communication:** `SUBMISSION.md` explains your design, failure modes you considered, and
   what you'd do next.

A clean, correct core beats a sprawling half-working stretch.

## Hints

- Before you write code, ask: *what happens if my process dies right here?* Ask it at every
  line between "pick a lead" and "the provider has accepted the call".
- `request_id` is your best friend. Decide when to generate it and where to store it.
- Start with `--failure-scale 0`, get the rules right, then turn the chaos back on.
