# Plan 001: Wire the orphaned test file into `npm test` and add the coverage 002/003/004 depend on

> **Executor instructions**: Follow this plan step by step. Run every verification
> command and confirm the expected result before moving to the next step. If anything
> in the "STOP conditions" section occurs, stop and report — do not improvise. When
> done, update the status row for this plan in `plans/README.md` — unless a reviewer
> dispatched you and told you they maintain the index.
>
> **Drift check (run first)**: `git diff --stat 53b4bd5..HEAD -- package.json test/`
> If any in-scope file changed since this plan was written, compare the "Current state"
> excerpts against the live code before proceeding; on a mismatch, treat it as a STOP
> condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: tests / dx
- **Planned at**: commit `53b4bd5`, 2026-06-17

## Why this matters

The repo has a test file, `test/noise-and-presence.test.js` (108 lines), that is **not
referenced by the `npm test` script**, so it never runs. A green `npm test` therefore
gives false confidence: a whole slice of presence/noise behavior is unverified. This
plan wires that file in and adds three small tests that plans 002/003/004 will rely on
to prove they didn't regress behavior. It is the verification baseline for the other
three plans — they should not run until this is DONE.

## Current state

- `package.json:9` — the `test` script runs four files and **omits** `noise-and-presence.test.js`:
  ```json
  "test": "node test/two-agents.test.js && node test/watch.test.js && node test/v2-watchers.test.js && node test/v3-features.test.js"
  ```
- `test/noise-and-presence.test.js` — exists, 108 lines, currently never executed by `npm test`.
- The test suite uses Node's built-in `node:test` + `node:assert` and spawns real
  server processes against a temp DB. **Exemplar to match for any new test:
  `test/v3-features.test.js`** — read it before writing tests; copy its harness
  (spawn helper, temp `INTERCOM_DB`, MCP stdio request/response helper, teardown).
- Tests pass today: `npm test` exits 0 with all checks green (verified during recon).
- Each test file is run as a standalone node script and is expected to **exit non-zero
  on failure** (that is how the `&&`-chained script reports failure).

## Commands you will need

| Purpose        | Command                                   | Expected on success                |
|----------------|-------------------------------------------|------------------------------------|
| Install deps   | `npm install`                             | exit 0 (needed in a fresh worktree)|
| Full suite     | `npm test`                                | exit 0, all checks green           |
| Single file    | `node test/noise-and-presence.test.js`    | exit 0                             |
| Single file    | `node test/v3-features.test.js`           | exit 0                             |

Runtime requires **node >= 22.5** (for `node:sqlite`). If `node --version` is below
that, STOP and report.

## Scope

**In scope** (the only files you may modify):
- `package.json` (the `test` script line only)
- `test/noise-and-presence.test.js` (only if it currently fails when run standalone — see Step 1)
- `test/v3-features.test.js` (append new tests here, OR create `test/regression-baseline.test.js` — see Step 2)

**Out of scope** (do NOT touch):
- `server.js`, `wait.js`, `watch.js`, `monitor-watcher.js` — this plan is tests-only. If a
  new test reveals a product bug, record it in NOTES and leave the source unchanged; the
  later plans fix behavior.
- The other four existing test files' contents.

## Git workflow

- Branch: `advisor/001-wire-orphaned-test`
- Commit style matches the repo (conventional commits — e.g. `test(intercom): ...`,
  `chore: ...`; see `git log --oneline`).
- Do NOT push or open a PR.

## Steps

### Step 1: Confirm the orphaned test passes standalone, then wire it in

First run it directly:

`node test/noise-and-presence.test.js` → expected exit 0.

- If it **passes**: add it to the `test` script in `package.json`. The script must run
  all five files, `&&`-chained, e.g.:
  ```json
  "test": "node test/two-agents.test.js && node test/noise-and-presence.test.js && node test/watch.test.js && node test/v2-watchers.test.js && node test/v3-features.test.js"
  ```
- If it **fails standalone**: it may be a stale test. Read it, and if the failure is a
  trivial drift (e.g. a changed output string), fix only the assertion to match current
  behavior — do NOT change source. If the failure looks like a real product bug, STOP
  and report (do not paper over it).

**Verify**: `npm test` → exit 0, and the output now includes checks from
`noise-and-presence.test.js`.

### Step 2: Add three regression tests the later plans depend on

Append to `test/v3-features.test.js` (or create `test/regression-baseline.test.js` using
the same harness as `v3-features.test.js`). Add these three tests, each asserting current
correct behavior so a later regression is caught:

1. **Topic-subscribed agent does NOT see an unsubscribed topic broadcast in `inbox`.**
   Agent A joins with `topics:["alerts"]`. Agent B broadcasts `send(message, topic:"other")`.
   A's `inbox` must NOT contain that message. (This is the behavior plan 002 must preserve
   end-to-end through the watchers.)

2. **Retention prune removes messages older than the cutoff on `join`.**
   Insert a message, then start a fresh server instance with `INTERCOM_RETENTION_DAYS=0`
   set to a value that prunes — OR, simplest: set env `INTERCOM_RETENTION_DAYS` and assert
   that with retention enabled and a back-dated `created_at`, a `join` drops it. If
   back-dating a row is too invasive for the harness, instead assert the simpler invariant:
   with `INTERCOM_RETENTION_DAYS=0` (disabled) no messages are pruned. Pick whichever the
   harness supports cleanly and note your choice in NOTES.

3. **A second live session requesting an already-held name gets a suffixed name.**
   Agent A joins as `worker`. While A is still alive, Agent B joins as `worker`. B's
   assigned name must be `worker-2` (not `worker`), and `who` must list both. (This pins
   the behavior plan 003 hardens.)

Model the spawn/teardown and the MCP request helper exactly on `test/v3-features.test.js`.
Each new test must assert something concrete (an actual value), not merely that a call
returned.

**Verify**: `npm test` → exit 0; output shows the 3 new checks passing. Confirm they
actually run (grep the output for their check names).

### Step 3: Confirm the full suite is green and self-contained

**Verify**:
- `npm test` → exit 0.
- `git status --porcelain` shows only the in-scope files changed.

## Test plan

- New tests live in `test/v3-features.test.js` (or `test/regression-baseline.test.js`),
  structurally modeled on `test/v3-features.test.js`.
- They cover: topic-routing-in-inbox (happy + the unsubscribed-topic exclusion),
  retention prune invariant, and live-name-collision suffixing.
- Verification: `npm test` → all pass, including the 3 new checks and the now-wired-in
  `noise-and-presence.test.js`.

## Done criteria

ALL must hold:

- [ ] `npm test` exits 0.
- [ ] `package.json` `test` script runs `noise-and-presence.test.js` (grep: `grep -c noise-and-presence package.json` → ≥1).
- [ ] The 3 new regression tests exist and appear in `npm test` output.
- [ ] No files outside the in-scope list are modified (`git status --porcelain`).
- [ ] `plans/README.md` row for 001 updated (unless your reviewer maintains the index).

## STOP conditions

Stop and report (do not improvise) if:

- `node --version` is below 22.5.0.
- `test/noise-and-presence.test.js` fails standalone for a reason that looks like a real
  product bug (not a trivial assertion drift).
- A new regression test cannot be made to pass against current behavior because current
  behavior is actually wrong — report the discrepancy; do not edit source to make it pass.
- `package.json:9` does not match the "Current state" excerpt (drift).

## Maintenance notes

- After this lands, any new test file MUST be added to the `npm test` script in the same
  commit — that omission is exactly the bug this plan fixes.
- Plans 002/003/004 rely on tests 1, 3 here respectively. A reviewer of those plans should
  confirm these tests still pass after the source changes.
