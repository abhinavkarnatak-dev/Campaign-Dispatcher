# AI Usage Disclosure

## Tools used
Claude (Anthropic).

## What I used them for
- Implementing the code, tests and docs, inside a step-by-step process that I set up and
  ran (see below).
- Explaining Temporal details (replay rules, signals, heartbeats).
- Debugging end-to-end runs against the provider.

## What I wrote or decided myself
- **Choosing Temporal over a database state machine:** the hard part of this assignment is surviving `kill -9` at any moment. With a database I would have to save every state change myself, scan for half-finished calls on restart, and run my own poller for due retries, callbacks and lost results. Temporal already stores the workflow state, timers and pending activity retries on its server, and a restarted worker replays the history and continues from the same point. The cost is running one extra server and following its rules in workflow code (no direct clock or network calls). I also made the local dev server the default, with Temporal Cloud used only when its env vars are set, so reviewers can run it either way.
- **Running the build step by step:** split the work into 10 small steps and required a written plan for each one (what is being built, why, which libraries, and how it fits the flow) before any code, so design choices were made before they were built. I decided when each step was ready to be committed, with one commit per step and the README kept up to date in each one.
- **Repo rules:** original files left unmodified, all code in `solution/`, no `.env`, databases or generated output (`final_state.csv`) in the repo. I checked these myself before pushing.
- **Testing on my own machine:** ran the unit tests, a full run and the 5-kill crash test on my Windows laptop. The numbers in `SUBMISSION.md` are from those runs.

## Where the AI was wrong or unhelpful
- A fixed 60-second safety margin before the calling window closed pushed a callback due at
  18:59 (window ends 19:00) to the next morning. Caught from `callback_lateness_minutes` in
  `/admin/report`, and changed to a much smaller margin.
- A workflow left over from an earlier stopped run kept placing calls against a new provider
  run and caused 1306 violations. Fixed by closing old workflows at startup.
- It gave me a wrong command to start the provider and a `winget` command that does not work on my laptop. I noticed when they failed.

## How I verified AI-generated code
- `pytest` (124 tests).
- `python -m dispatcher verify` after full runs: every lead matches `/admin/leads` and
  `violation_count` is 0.
- The crash test (`tools/chaos.py`), which kills the process 5 times while calls are in flight.
