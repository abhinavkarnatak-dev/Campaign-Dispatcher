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
│   ├── config.py     # loads campaigns, leads and runtime settings
│   └── clock.py      # the provider's simulated clock
└── tests/            # pytest tests, no network needed
```

## How time works

The provider runs a simulated clock, 600 times faster than real time by default. All business rules use that simulated time, never the machine's clock.

The dispatcher reads the clock's starting point and speed once from `GET /v1/clock`, then computes simulated time itself. Inside the workflow it uses `workflow.now()` as the real time, because Temporal gives back the same value when it replays the workflow after a crash.

Customer names in the lead files are never loaded. They are not needed, and anything the workflow holds is stored in Temporal's history.
