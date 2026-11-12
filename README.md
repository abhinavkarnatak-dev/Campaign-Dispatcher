# Agentic Universe: Junior SDE Take-Home (Data & Workflow)

Thanks for your interest in Agentic Universe. We build AI voice agents for insurers, lenders and
consumer brands across India and APAC. Our platform has three layers:

- **Data:** leads, call events, dispositions and the interaction ledger that every client report
  is built on.
- **Workflow:** campaigns, scheduling, dialling, retries and callbacks, all orchestrated durably.
- **Application:** dashboards, consoles and APIs.

This take-home focuses on **data** and **workflow**. **Pick one assignment.** Don't do both.

| | Assignment | You'll mostly be doing |
|---|---|---|
| **A** | [Interaction Ledger & Reporting Pipeline](assignment-a-data/README.md) | Messy-data ingestion, schema design, idempotent pipelines, exact metrics |
| **B** | [Durable Campaign Dispatcher](assignment-b-workflow/README.md) | State machines, idempotency, failure handling, scheduling under constraints |

Both are judged on the same bar. Pick the one that excites you more.

## Ground rules

- **Time box:** plan for about **6–8 focused hours**. You have **72 hours** from when you receive
  this. We know you have a life. If you run out of time, stop and tell us in `SUBMISSION.md`
  what you'd do next. A well-explained partial solution beats a rushed complete one.
- **Language:** Python 3.11+. Use any libraries you like; list them in `requirements.txt`.
- **AI tools** (Claude, Cursor, Copilot, ChatGPT...) are **allowed**, as they would be on the
  job. You must fill in [`AI_USAGE.md`](AI_USAGE.md) honestly. In the follow-up interview we'll
  ask you to walk through your code and change it live. Submit only code you understand and can
  defend.
- **Ambiguity is intentional in places.** When the spec doesn't decide something, make a
  reasonable call and write it down in `SUBMISSION.md`. Don't wait for an answer from us.
- **Synthetic data only.** Everything here is generated. No real customer data is involved.

## Getting started

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# then follow your chosen assignment's README
```

`docker-compose.yml` starts a local Postgres if you want one (`docker compose up -d`). SQLite
is equally acceptable.

## What to submit

1. Push your work to a **private** GitHub or GitLab repository. Start from this repo and keep
   its structure. Put your code in `solution/` (or a similar top-level folder) and keep the
   original files unmodified.
2. Your repo must include:
   - `SUBMISSION.md`: fill in the template provided
   - `AI_USAGE.md`: fill in the template provided
   - Exact setup and run commands that work from a clean clone
   - Tests, runnable with one command (for example `pytest`)
3. Give read access to **`[[REVIEWER_HANDLES]]`** and email the repo link to
   **`[[SUBMISSION_EMAIL]]`** before the deadline.

Please **don't** publish your solution publicly, and **don't** commit generated data, `.env`
files, or databases.

## What happens next

If your submission clears the bar, we'll schedule a **45-minute technical conversation**:

- 15 min: you walk us through your design and the decisions in `SUBMISSION.md`
- 20 min: we change a requirement and you extend your code live (AI tools allowed; screen
  share)
- 10 min: your questions for us

## What we look for

| Signal | What good looks like |
|---|---|
| Correctness | It produces the right answer on data and seeds you haven't seen |
| Robustness | It behaves sensibly when inputs are dirty, things fail, or it's run twice |
| Design | Clear data and state model; you can explain why it's safe |
| Code quality | Readable, organised, tested where it matters |
| Communication | Honest about trade-offs, assumptions and what's missing |
| Ownership | You made decisions instead of waiting to be told |

Good luck. We're looking forward to reading your work.
