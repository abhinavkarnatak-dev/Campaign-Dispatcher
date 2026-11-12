# Assignment A: Interaction Ledger & Reporting Pipeline (Data)

## Context

Agentic Universe runs outbound voice campaigns for insurers and consumer brands. Every campaign
lives inside a **workflow** (for example, `motor_renewal`). Data reaches us in two ways:

1. **Lead uploads.** Operations teams upload CSV files of the customers to call. Each CSV is a
   **batch**, and each workflow has its own **schema** that says which columns matter.
2. **Call events.** The calling system emits a stream of events (`call.initiated`,
   `call.connected`, `disposition.set`, ...) as JSON Lines files.

From these we build the reports our clients look at every morning: how many leads were reached,
how many calls connected, how many customers renewed. **If these numbers are wrong, the client
loses trust in everything else we do.** Your job is to build the pipeline that produces them.

Read [`../docs/GLOSSARY.md`](../docs/GLOSSARY.md) first. It takes five minutes.

## What you're given

```
assignment-a-data/
├── schemas/
│   ├── motor_renewal.json      # lead schema for each workflow
│   ├── policy_feedback.json
│   └── dispositions.csv        # disposition catalogue: which outcomes are terminal / success
└── tools/
    └── generate_data.py        # synthetic data generator (stdlib only)
```

Generate the data:

```bash
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
# Optional, for the performance stretch goal (~140k leads, ~600k events):
python assignment-a-data/tools/generate_data.py --seed 42 --scale large --out data_large
```

This produces two **drops**. Each drop simulates one day's delivery:

```
data/
├── drop_1/
│   ├── leads/<workflow_id>/batch_001.csv, batch_002.csv
│   └── events/events_0001.jsonl
└── drop_2/
    ├── leads/<workflow_id>/batch_003.csv
    └── events/events_1001.jsonl
```

> Do not commit the generated data. We will run your pipeline on **a different seed**.

## What to build

A pipeline you can run like this (the exact interface is up to you; document it):

```bash
<your-command> ingest data/drop_1
<your-command> ingest data/drop_2
<your-command> report --out reports/
```

It must:

1. **Ingest leads** against the workflow schema: map headers, validate and normalise values,
   reject bad rows with a reason, and merge duplicates.
2. **Ingest events** into an **append-only interaction ledger**: dedupe, link each event to its
   lead, and quarantine what can't be linked.
3. **Store data in a real database.** SQLite or PostgreSQL are both fine. A `docker-compose.yml`
   with Postgres is included at the repo root if you want it. Design the schema yourself and
   explain it.
4. **Protect PII at rest.** Fields marked `"pii": true` must not be stored in plaintext. You still
   need to match events to leads by phone number, so think about how.
5. **Be idempotent and incremental.** Re-running `ingest` on a drop you already ingested must
   change nothing. Ingesting `drop_1` then `drop_2` must give the same reports as ingesting
   everything at once.
6. **Produce the five report files** described in the output contract below, exactly in the
   format given. We diff them against expected output.

## Rules (the spec)

These rules are deliberately precise. When something is genuinely ambiguous, make a call and
write it down in `SUBMISSION.md`. That is part of what we assess.

### Lead files

- **Header matching:** to normalise a header, lower-case it, trim it, and replace every run of
  `_` or whitespace with a single space. Match the result against the field `name` or any of its
  `aliases` (normalised the same way). Ignore columns that match nothing.
- **Blank rows** (every cell empty or whitespace) are skipped and **not counted**.
- **Row numbers** count non-blank data rows within a file, starting at 1. The header is not
  counted.
- Values are trimmed. An empty value counts as missing.
- **Field types:**
  - `string`: the trimmed value.
  - `phone`: an Indian mobile number. Strip every non-digit. If 12 digits starting with `91`,
    drop the `91`. If 11 digits starting with `0`, drop the `0`. The result is valid only if it
    is exactly 10 digits and starts with 6, 7, 8 or 9. Store it canonically as `+91XXXXXXXXXX`.
  - `date`: accept `YYYY-MM-DD`, `DD/MM/YYYY`, `DD-MM-YYYY` and `DD-Mon-YYYY` (for example
    `01-Dec-2026`; the month name is case-insensitive). Dates are **day-first**. Impossible
    dates (like 31/02) are invalid.
  - `decimal`: optionally prefixed by `₹`, `Rs`, `Rs.` or `INR` (case-insensitive, optional
    space). Commas are thousands separators. Must be ≥ 0. Round to 2 places.
  - `enum`: compared case-insensitively against `values` and stored lower-case.
- **Rejection:** walk the fields **in schema order**. The first failure becomes the row's single
  reason:
  - a required field is missing → `missing_required:<field_name>`
  - the phone is present but invalid → `invalid_phone`

  Rejected rows are not leads. They go to `rejections.csv`.
- **Optional field present but invalid** → store null and count one **field warning**. Count
  warnings only for rows that are not rejected.
- **Lead identity** is `(workflow_id, canonical phone)`.
- **Duplicates:** process drops in the order ingested, files in filename order and rows in file
  order. The first valid row for an identity creates the lead, and its file stem
  (`batch_001`) becomes the lead's `batch_id` permanently. Each later valid row for the same
  identity counts as one **merged duplicate** and overwrites only the fields where its value is
  non-null (last write wins).
- **Defaults** (for example `preferred_language → en`) apply after merging, to fields that are
  still null.

### Event files

Each line is one JSON event:

```json
{"event_id": "evt_00001323", "event_type": "call.telephony_failed", "workflow_id": "motor_renewal",
 "call_id": "call_00000414", "occurred_at": "2026-10-05T13:20:21Z", "phone": "91-77187-50036",
 "payload": {"reason": "carrier_rejected"}}
```

| `event_type` | Meaning | Payload |
|---|---|---|
| `call.initiated` | We placed a call | `{}` |
| `call.telephony_failed` | The carrier/telephony layer failed; the customer's phone never rang | `reason` |
| `call.connected` | The customer picked up | `{}` |
| `call.ended` | The call ended | `duration_sec`, `end_reason` |
| `disposition.set` | The voice agent classified the call's outcome | `disposition_code` |
| `disposition.override` | A human supervisor corrected the outcome. **Has no `phone`, only `call_id`.** | `disposition_code`, `overridden_by`, `reason` |

Process each line in this order:

1. **Malformed:** the line is not valid JSON, or has no `event_id` → count it and skip it.
2. **Duplicate:** an `event_id` already seen (in any drop) → count it and skip it.
3. **Orphan:** the event can't be linked to a valid lead → count it and keep it out of every
   report. An event links via `(workflow_id, normalised phone)` using the phone rules above.
   A `disposition.override` links through its `call_id` to the lead of that call's other events.
4. **Unknown disposition:** a `disposition.*` event whose code is not in `dispositions.csv` for
   that workflow → count it and ignore it.

**Timestamps** come in three forms: UTC (`...Z`), offset (`...+05:30`), and naive
(`2026-10-03 10:01:00`). **Naive means IST.** All dates in reports are **IST** dates.

> **Heads-up:** events can arrive **before** the lead they belong to (in an earlier drop). Reports
> must always reflect **everything ingested so far**. An event that was an orphan after `drop_1`
> must count once its lead arrives in `drop_2`. Orphan counts in `data_quality.json` reflect the
> final state.

### Derived facts

- A **call** is counted only if its `call.initiated` event is present (and linked). The call's
  date is the IST date of `call.initiated`.
- A call is **connected** if it has a `call.connected` event.
- A call's **effective disposition** is the code of its latest `disposition.override` by
  `occurred_at`. If it has no override, it's the latest `disposition.set`. Otherwise it has none.
  **Overrides never delete or edit the original event.** The ledger stays append-only.
- A lead's **bucket** is the first of these that matches:
  1. `yet_to_reach_out`: no calls.
  2. `completed`: any call's effective disposition is terminal (`is_terminal=true`).
  3. `in_progress_telephony_failed`: every call has a `call.telephony_failed` event.
  4. `in_progress_dialed`: everything else.

## Output contract

`report --out <dir>` writes these five files. CSVs use a header row, `,` as the delimiter, `\n`
or `\r\n` line endings, and no index column. Booleans are `true`/`false`. Sort exactly as stated.

**1. `batch_summary.csv`**, sorted by `workflow_id, batch_id`
```
workflow_id,batch_id,total_leads,yet_to_reach_out,in_progress_telephony_failed,in_progress_dialed,completed
```

**2. `daily_overview.csv`**: one row per workflow per IST date that has at least one call, sorted
by `workflow_id, date`
```
workflow_id,date,total_calls,connected_calls,connectivity_rate,success_leads
```
- `connectivity_rate` = `connected_calls / total_calls`, formatted with exactly 4 decimals
  (`0.5625`).
- `success_leads` = distinct leads with at least one call **on that date** whose effective
  disposition has `is_success=true`.

**3. `lead_report.csv`**: one row per lead, sorted by `workflow_id, external_ref`
```
workflow_id,external_ref,batch_id,phone_masked,bucket,total_calls,connected_calls,last_call_at,current_disposition,ever_success
```
- `external_ref`: the value of the schema field marked `"reference": true`.
- `phone_masked`: `+91******` followed by the last 4 digits.
- `last_call_at`: the latest `call.initiated` time as ISO-8601 in IST, for example
  `2026-10-14T17:55:24+05:30`. Empty if there are no calls.
- `current_disposition`: the effective disposition of the most recent call (by initiated time)
  that has one. Empty if none.
- `ever_success`: whether any call's effective disposition has `is_success=true`.

**4. `rejections.csv`**, sorted by `workflow_id, batch_id, row_number`. **No PII.**
```
workflow_id,batch_id,row_number,reason
```

**5. `data_quality.json`**
```json
{
  "lead_rows_total": 0,
  "lead_rows_rejected": 0,
  "rejections_by_reason": {"invalid_phone": 0, "missing_required:customer_name": 0},
  "duplicate_lead_rows_merged": 0,
  "field_warnings": 0,
  "events_total": 0,
  "malformed_events": 0,
  "duplicate_events_dropped": 0,
  "orphan_events": 0,
  "unknown_disposition_events": 0
}
```
`events_total` counts every non-empty line read, including malformed and duplicate lines.

## Scope

**Core (expected within the time box):**
- Lead ingestion with validation, normalisation, rejection and merging
- Event ingestion with dedupe, linking and orphan handling
- Database schema with an append-only ledger
- PII not stored in plaintext
- All five report files matching the contract
- Idempotent re-ingest
- Tests for the rules most likely to break: phone normalisation, dates, effective disposition,
  bucket logic
- `SUBMISSION.md` and `AI_USAGE.md` filled in

**Stretch (pick any; don't do them all):**
- Truly incremental processing: drop 2 doesn't re-process drop 1's work. Explain the trade-off.
- Performance: the `--scale large` dataset ingests and reports in under 2 minutes on a laptop.
  Report your timings.
- Do the report aggregation in SQL rather than in application code.
- A short note on how you'd run this in production for 100k leads/day: scheduling, failures,
  backfills.

## What makes this data messy

Phones in six formats, invalid phones, missing required fields, duplicates across batches,
renamed and re-ordered columns in a later batch, extra columns, blank rows, mixed date and
currency formats, events out of order, duplicate events (including across drops), malformed
lines, events for numbers never uploaded, events that arrive before their lead, overrides with
no phone, and dispositions not in the catalogue. Every one is intentional.

## How we evaluate

1. **Correctness:** we run your pipeline on a fresh seed and diff against expected output.
2. **Data modelling:** your schema, constraints, ledger design, and PII approach.
3. **Robustness:** idempotency, behaviour on re-runs and bad input.
4. **Code quality and tests:** readable, structured, and tested where it matters.
5. **Communication:** `SUBMISSION.md` explains your decisions, trade-offs and what you'd do next.

A clean, correct core beats a sprawling half-working stretch.
