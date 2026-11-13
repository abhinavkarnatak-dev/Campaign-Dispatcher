# Campaign Dispatcher

A durable outbound call dispatcher for Agentic Universe's Assignment B. It decides who to call and when, places calls through the telephony provider, processes the results, and keeps going through crashes, lost webhooks and provider errors.

It is built on **Temporal**: every decision is saved in Temporal's history, so after a `kill -9` the worker replays that history and continues exactly where it stopped.

## Requirements

- Python 3.11 or newer
- Temporal CLI, for the local dev server ([install guide](https://docs.temporal.io/cli#install))

No account or API key is needed. Everything runs locally.

## Setup

From the repo root:

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Run it

Three terminals, all from the repo root with the virtual environment active.

**1. Temporal server**

```bash
cd solution
temporal server start-dev --db-filename temporal.db
```

The database file keeps Temporal's data if the server itself is restarted. It is created inside `solution/`, where git ignores it. Temporal's web UI is at http://localhost:8233, where you can watch the workflow and every step it takes.

**2. The provider**

```bash
cd assignment-b-workflow
python -m mock_telephony
```

**3. The dispatcher**

```bash
cd solution
python -m dispatcher run
```

It runs until every lead reaches a final state, writes `final_state.csv` in the repo root, prints a summary and exits. While it runs, http://127.0.0.1:8200/status shows progress per campaign.

**Restarting** is the same command. If the process is killed (even with `kill -9`), running `python -m dispatcher run` again re-attaches to the same workflow and continues. Against a brand new provider run, it starts a fresh workflow. If the workflow has already finished, it just reads the result.

**4. Check the result**

```bash
cd solution
python -m dispatcher verify
```

This compares `final_state.csv` with the provider's `/admin/leads`, lead by lead, and shows the provider's `/admin/report` (violations, concurrency, callback lateness, webhook counts). It ends with `RESULT: PASS` or `RESULT: FAIL`.

**Crash test**

```bash
cd solution
python tools/chaos.py
```

Runs the dispatcher, hard-kills it 5 times at random moments while calls are in flight (SIGKILL, the same as `kill -9`, on Linux and macOS, `TerminateProcess` on Windows), stays down 10 seconds each time so webhooks sent meanwhile are lost, restarts it, and runs `verify` at the end. `python tools/chaos.py --help` shows the options. Start it instead of step 3, with a fresh provider.

## Run the tests

```bash
cd solution
pytest
```

The scheduling and provider tests use a fake clock and a fake provider, so they never wait on anything. The workflow tests start a real local Temporal server automatically (downloaded once on the first run, about 50 MB) and are skipped if it cannot start.

## Configuration

Nothing needs to be set to run locally. Every setting has a default.

| Variable | Default | What it is |
|---|---|---|
| `PROVIDER_URL` | `http://127.0.0.1:8099` | The telephony provider |
| `FIXTURES_DIR` | `assignment-b-workflow/fixtures` | Where `campaigns.json` and the lead CSVs are |
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal server |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `TEMPORAL_API_KEY` | empty | Set this only for Temporal Cloud |
| `TASK_QUEUE` | `campaign-dispatcher` | Temporal task queue |
| `WEBHOOK_HOST` / `WEBHOOK_PORT` | `127.0.0.1` / `8200` | Where the provider sends call results |
| `OUTPUT_PATH` | `final_state.csv` in the repo root | Where the final report is written |

To use **Temporal Cloud** instead of the local server, copy `solution/.env.example` to `solution/.env` and fill in `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` and `TEMPORAL_API_KEY`. The `.env` file is ignored by git.

## Project structure

```
solution/
├── dispatcher/
│   ├── config.py      # loads campaigns, leads and runtime settings
│   ├── clock.py       # the provider's simulated clock
│   ├── scheduling.py  # the business rules: windows, waits, priority, limits, final states
│   ├── provider.py    # client for the provider API, safe retries with one request_id
│   ├── activities.py  # Temporal activities: place_call and get_call
│   ├── workflow.py    # the coordinator workflow that owns every lead
│   ├── webhooks.py    # webhook receiver: checks signatures, signals the workflow
│   ├── output.py      # writes final_state.csv, and the verify check
│   ├── main.py        # runs receiver, worker and workflow in one process
│   └── __main__.py    # command line: python -m dispatcher run / verify
├── tools/
│   └── chaos.py       # crash test: random hard kills, restarts, then verify
└── tests/             # pytest tests
```

## How time works

The provider runs a simulated clock, 600 times faster than real time by default. All business rules use that simulated time, never the machine's clock.

The dispatcher reads the clock's starting point and speed once from `GET /v1/clock`, then computes simulated time itself. Inside the workflow it uses `workflow.now()` as the real time, because Temporal gives back the same value when it replays the workflow after a crash.

Customer names in the lead files are never loaded. They are not needed, and anything the workflow holds is stored in Temporal's history.

## Scheduling rules

All business rules live in `dispatcher/scheduling.py` as pure functions: they take the current simulated time and the lead states, and return decisions. No network, no clock reading. This makes them safe to run inside a Temporal workflow and easy to test.

| Rule | How it is applied |
|---|---|
| Calling window (R1) | A call starts only inside both the campaign window and 09:00-21:00 IST, on an allowed day. The provider's 2 minute grace is never relied on, and no call starts in the last few simulated seconds before a window closes (see below) |
| Concurrency (R2) | Active calls are counted per tenant, across all of that tenant's campaigns |
| One call per phone (R3) | A phone with an active call is skipped, in every campaign |
| Attempts (R4) | A lead with `max_attempts` accepted calls is never dialled again |
| Retries (R5) | Next attempt no earlier than the provider's `ended_at` plus 60 minutes (`no_answer`, `busy`) or 15 minutes (`failed`) |
| Callbacks (R6) | Next attempt no earlier than `callback_at`, or the next window opening if that falls outside the window |
| Priority (R6) | Due callbacks first, then due retries, then fresh leads. Inside each group, the one waiting longest goes first |
| Terminal and DND (R7, R8) | A terminal disposition finishes the lead. `dnd` blocks that phone in every campaign |
| Final state (R9) | `completed` > `exhausted` > `dnd_blocked`. A lead with a call in flight is never finalised until that call's result arrives |

**Close margin.** The activity checks the window again right before every request, so the margin only has to cover the real time between that check and the provider reading its own clock: one local HTTP request. It is 10 real milliseconds converted to simulated time, and at least 15 simulated seconds (15 seconds at the default 600x). It stays under a minute on purpose: `callback_at` is always on a whole minute, so a callback due at 18:59 for a window closing at 19:00 still gets its call that day instead of the next morning.

**Slots held for callbacks.** If a tenant has callbacks that become due within the next 7 simulated minutes (the longest possible call), that many of its slots are kept free from retries and fresh leads. When the callback is due, a slot is free right away instead of waiting for a call to end. On the same seeds, this and the request timeout below cut callback lateness from p90 12 to 16 minutes down to 5 to 6 minutes, with no change in how long the whole campaign takes.

If an attempt to place a call fails (the provider keeps answering 503, or rejects it), the lead waits a short time before it can be picked again (1 simulated minute, or 5 after a rejection), so a failing lead cannot make the dispatcher hammer the provider.

## Placing calls safely

Every call attempt gets a `request_id` (for example `c_acme_renewals:ren_0001:d2`) chosen by the workflow **before** anything is sent, and saved in Temporal's history. Every retry of that attempt uses the same `request_id`, and the provider places at most one call per `request_id`. So a retry after an error, a timeout or a crash can never place a second call.

`place_call` turns every provider response into one of three answers:

| Provider response | Answer |
|---|---|
| `201`, or `200` (this `request_id` already exists) | `accepted` |
| `503` | Retried a few times with the same `request_id`, then `not_placed` |
| `504`, a network error, or no answer within 0.5 real seconds (outcome unknown) | Look the call up with `GET /v1/calls?request_id=...`. Found: `accepted`. Not found: retried with the same `request_id` |
| `409`, `422`, `429` (a rule was broken) | `rejected`, never retried |

A normal answer to `POST /v1/calls` takes a few milliseconds, so a request that takes longer than 0.5 real seconds is abandoned and handled like a `504`. Waiting it out is expensive: the provider sometimes hangs for 2 real seconds before answering `504`, which is 20 simulated minutes at 600x, with the lead holding a call slot the whole time.

Right before every request, including retries, the activity checks the calling window again using the real current time. If the window has closed, nothing is sent and the answer is `not_placed`. If a lookup itself fails, the error goes up to Temporal, which retries the whole activity later with the same `request_id`.

## The workflow

One Temporal workflow, `CampaignDispatcher`, owns all the leads. One coordinator is used instead of one workflow per lead because three rules are global: the tenant limit is shared across campaigns, a phone may not be called twice at once across campaigns, and DND blocks a phone everywhere. With every lead in one place, these checks are simple and cannot race.

The loop:

1. Work out the simulated time from `workflow.now()`.
2. Finalise every lead that is done.
3. Pick the leads to call now (`pick_next`).
4. For each pick, mark the lead active (its tenant slot and phone are reserved at once), give it a new `request_id`, and start `place_call` in the background.
5. Sleep until the next lead becomes ready, or until something happens (a call result arrives or an activity finishes).

Call results come in as the `call_result` signal:

- A result whose `call_sid` was already applied is ignored, so duplicate webhooks are harmless.
- A result can arrive before `place_call` has returned (the webhook beat the `201`). The result proves the call was accepted, so the attempt is counted right then, and the later activity answer sees it was already counted.
- A `dnd` result adds the phone to the DND list, and any other lead with that phone that is not completed or exhausted becomes `dnd_blocked`.

Progress can be read at any time with the workflow's `status` query.

**Why it survives a crash:** every decision, activity result, signal and timer is stored in Temporal's history. A restarted worker replays that history through the same code, rebuilds exactly the same state, and continues. To keep replay exact, the workflow only reads time from `workflow.now()`, never touches the network (activities do), and never makes a decision based on the order of a Python set. The campaign config is passed in without the webhook secret, because workflow inputs are stored in history.

## Webhooks and restarts

The provider sends each call result to `POST /webhooks/telephony`. The receiver checks the `X-AU-Signature` header (HMAC-SHA256 of the raw body, compared in constant time) and passes the result into the workflow as the `call_result` signal. Once Temporal accepts a signal it is stored on the Temporal server, so it is safe even if the worker is down at that moment.

| Case | Response |
|---|---|
| Valid signature, result handed to Temporal | `200` |
| Missing or wrong signature | `401` |
| Body is not a valid call result | `400` |
| Temporal could not be reached | `503`, so the provider redelivers |

The receiver, the Temporal worker and the workflow run in one process. The receiver starts first, so it is already listening when the first call is placed. The webhook secret stays in the receiver and is never passed to the workflow.

The workflow ID includes the provider's clock start time, for example `campaign-dispatch-1794397432`. This gives one workflow per provider run:

| Situation | What happens |
|---|---|
| Restart after a crash, same provider run | Same ID, so the dispatcher re-attaches to the running workflow |
| A brand new provider run | New ID, so a fresh workflow starts |
| Restart after the workflow finished | Temporal refuses a duplicate, so the dispatcher reads the stored result |

So a restart can never start a second workflow that would dial the same leads again.

**A workflow left over from an earlier provider run.** If a run is stopped partway and a new provider run is started against the same Temporal server, the old workflow is still "running" in Temporal. Without care, the new worker would pick it up and place calls against the new provider using the old run's state. Two things prevent that:

1. At startup, before the worker starts, the dispatcher terminates any other running `CampaignDispatcher` workflow on its task queue, and logs that it did.
2. Every activity checks which workflow called it. For any workflow other than the current provider run's, it fails permanently without contacting the provider.

**Temporal server restarts.** The workflow's state lives in the Temporal server's database (`--db-filename`), so a restart of the server loses nothing. While it is down, the worker keeps reconnecting, webhooks get a `503` (the provider redelivers, and the watchdog catches anything it gives up on), and the dispatcher keeps waiting for the result instead of exiting.

## Lost results

A call's result can fail to reach the dispatcher in two ways: the provider sometimes never sends the webhook, and a webhook sent while the dispatcher is down is given up after the provider's retries (about 7 real seconds).

Every accepted call therefore has a watchdog in the workflow:

1. Wait until the call must have ended. The provider's longest call is 45 seconds of ringing plus 6 minutes of talk, so the watchdog waits 8 simulated minutes (at least 1 real second).
2. If the result has arrived by then, stop.
3. Otherwise ask the provider with `GET /v1/calls/{call_sid}` and apply the result exactly like a webhook. If the call is somehow still active, ask again every 2 real seconds.

A webhook that arrives after its result was recovered is ignored, because its `call_sid` has already been applied. The watchdog's timer and lookup are part of the workflow, so after a restart every waiting watchdog carries on, and catches the results that were sent while the dispatcher was down. The number of results found this way is shown as `results_recovered` in `/status` and in the final summary.

The watchdog checks soon after the call ends instead of waiting out the provider's 7 second retry window, because at 600x speed 7 real seconds is 70 simulated minutes of a lead holding a call slot.

**A crash during `place_call`:** the activity sends a heartbeat to Temporal before every HTTP request. If the worker is killed mid-request, the heartbeats stop and Temporal runs the activity again within 10 seconds, with the same `request_id`, so the provider returns the existing call instead of placing a new one.

## Output

`final_state.csv` has one row per lead, sorted by `campaign_id, lead_id`:

```
campaign_id,lead_id,final_state,attempts,last_status,last_disposition
c_acme_feedback,fb_0001,completed,2,completed,interested
c_acme_feedback,fb_0002,exhausted,3,no_answer,
```

`attempts` counts the calls the provider accepted. `last_status` and `last_disposition` come from the lead's most recent ended call and are empty if there was none. Lines end with `\n`.

The file is written to a temporary file first and then renamed into place, so a crash while writing never leaves a half written file. If the process dies after the workflow finished but before the file was written, running `python -m dispatcher run` again reads the finished workflow's stored result and writes the file, without placing any calls.
