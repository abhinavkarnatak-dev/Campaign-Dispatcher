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
│   └── workflow.py    # the coordinator workflow that owns every lead
└── tests/            # pytest tests, no network needed
```

## How time works

The provider runs a simulated clock, 600 times faster than real time by default. All business rules use that simulated time, never the machine's clock.

The dispatcher reads the clock's starting point and speed once from `GET /v1/clock`, then computes simulated time itself. Inside the workflow it uses `workflow.now()` as the real time, because Temporal gives back the same value when it replays the workflow after a crash.

Customer names in the lead files are never loaded. They are not needed, and anything the workflow holds is stored in Temporal's history.

## Scheduling rules

All business rules live in `dispatcher/scheduling.py` as pure functions: they take the current simulated time and the lead states, and return decisions. No network, no clock reading. This makes them safe to run inside a Temporal workflow and easy to test.

| Rule | How it is applied |
|---|---|
| Calling window (R1) | A call starts only inside both the campaign window and 09:00-21:00 IST, on an allowed day. The provider's 2 minute grace is never relied on, and no call starts in the last 60 simulated seconds before a window closes |
| Concurrency (R2) | Active calls are counted per tenant, across all of that tenant's campaigns |
| One call per phone (R3) | A phone with an active call is skipped, in every campaign |
| Attempts (R4) | A lead with `max_attempts` accepted calls is never dialled again |
| Retries (R5) | Next attempt no earlier than the provider's `ended_at` plus 60 minutes (`no_answer`, `busy`) or 15 minutes (`failed`) |
| Callbacks (R6) | Next attempt no earlier than `callback_at`, or the next window opening if that falls outside the window |
| Priority (R6) | Due callbacks first, then due retries, then fresh leads. Inside each group, the one waiting longest goes first |
| Terminal and DND (R7, R8) | A terminal disposition finishes the lead. `dnd` blocks that phone in every campaign |
| Final state (R9) | `completed` > `exhausted` > `dnd_blocked`. A lead with a call in flight is never finalised until that call's result arrives |

If an attempt to place a call fails (the provider keeps answering 503, or rejects it), the lead waits a short time before it can be picked again (1 simulated minute, or 5 after a rejection), so a failing lead cannot make the dispatcher hammer the provider.

## Placing calls safely

Every call attempt gets a `request_id` (for example `c_acme_renewals:ren_0001:d2`) chosen by the workflow **before** anything is sent, and saved in Temporal's history. Every retry of that attempt uses the same `request_id`, and the provider places at most one call per `request_id`. So a retry after an error, a timeout or a crash can never place a second call.

`place_call` turns every provider response into one of three answers:

| Provider response | Answer |
|---|---|
| `201`, or `200` (this `request_id` already exists) | `accepted` |
| `503` | Retried a few times with the same `request_id`, then `not_placed` |
| `504` or a network error (outcome unknown) | Look the call up with `GET /v1/calls?request_id=...`. Found: `accepted`. Not found: retried with the same `request_id` |
| `409`, `422`, `429` (a rule was broken) | `rejected`, never retried |

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
