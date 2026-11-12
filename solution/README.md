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

The tests use a fake clock, so they never wait on the provider or on Temporal.

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
│   └── scheduling.py  # the business rules: windows, waits, priority, limits, final states
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
