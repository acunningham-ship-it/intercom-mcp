# Plan 004: Harden presence against PID reuse

> **Executor instructions**: Follow this plan step by step. Run every verification
> command and confirm the expected result before moving to the next step. If anything
> in the "STOP conditions" section occurs, stop and report — do not improvise. When
> done, update the status row for this plan in `plans/README.md` — unless a reviewer
> dispatched you and told you they maintain the index.
>
> **Drift check (run first)**: `git diff --stat 24ead7d..HEAD -- server.js`
> If `server.js` changed since this plan was written, compare the "Current state" excerpts
> against the live code before proceeding; on a mismatch, treat it as a STOP condition.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: MED
- **Depends on**: plans/001 (DONE)
- **Category**: bug (reliability)
- **Planned at**: commit `24ead7d`, 2026-06-17

## Why this matters

Presence ("is this agent online?") is decided solely by `process.kill(pid, 0)` — does a
process with that PID exist. PIDs are reused by the OS. When a session dies and its PID is
later assigned to an unrelated process, the dead agent's row reads as **online**: `who`
lists a ghost, and worse, `send`/`ask` validate the recipient against `onlineAgents()` and
will happily accept a directed message to a name whose owning process is gone — the message
lands in an inbox **nobody is reading**. On a box that churns through many short-lived agent
sessions (a fleet), PID reuse is not exotic. The fix is to verify identity, not just PID
existence: store something that uniquely identifies the *specific* process at join time and
check it before trusting the PID.

## Current state

- `server.js:95-102` — liveness is PID-only:
  ```js
  function pidAlive(pid) {
    try { process.kill(pid, 0); return true; }
    catch (err) { return err.code === "EPERM"; }
  }
  ```
- `server.js:172-180` — `onlineAgents()` trusts `pidAlive` and prunes rows whose pid is "dead":
  ```js
  function onlineAgents() {
    const rows = db.prepare("SELECT * FROM agents ORDER BY joined_at").all();
    const online = [];
    for (const a of rows) {
      if (pidAlive(a.pid)) online.push(a);
      else db.prepare("DELETE FROM agents WHERE name = ?").run(a.name);
    }
    return online;
  }
  ```
- The `agents` table (`server.js:32-41`) stores `pid` but **no per-process identity token**.
- Additive migrations already exist (`server.js:62-73`) — the pattern is
  `try { db.exec("ALTER TABLE agents ADD COLUMN <col>") } catch {}`. **Add new columns the
  same way; never drop/rename.**
- A stable per-process identity is available on Linux from `/proc/<pid>/stat` field 22
  (process start time, in clock ticks since boot) — two processes with the same PID at
  different times have different start times. Node also exposes `process.pid`. The boot id
  is at `/proc/sys/kernel/random/boot_id`.

## Commands you will need

| Purpose      | Command                              | Expected on success |
|--------------|--------------------------------------|---------------------|
| Install deps | `npm install`                        | exit 0              |
| Full suite   | `npm test`                           | exit 0, all green   |
| Presence test| `node test/noise-and-presence.test.js` | exit 0            |

Runtime requires node >= 22.5. Target platform is Linux (this box) — `/proc` is available.

## Scope

**In scope**:
- `server.js` — add a `pid_start` (process start-time token) column via the existing
  migration block; capture it at join; verify it in liveness.
- `test/noise-and-presence.test.js` (or a new `test/pid-reuse.test.js`) — the reuse test.

**Out of scope**:
- `wait.js`, `monitor-watcher.js`, `watch.js` — they each have their own `pidAlive` for the
  dashboard; leave them (a follow-up could share this, but not in this plan).
- Removing the existing `pidAlive` PID check — keep it as the fast first gate; the start-time
  check is an *additional* confirmation, not a replacement.

## Git workflow

- Branch: `advisor/004-harden-presence-pid-reuse`
- Conventional commits (e.g. `fix(intercom): verify process identity to survive PID reuse`).
- Do NOT push or open a PR.

## Steps

### Step 1: Add a `pid_start` column (additive migration)

In the migration block (`server.js:62-68`, the `for (const col of [...])` over `agents`),
add `"pid_start TEXT"` to the list. This is safe on existing DBs (guarded `try/catch`).

**Verify**: start a server against a temp DB and confirm no error; `node -e` opening the DB
and `PRAGMA table_info(agents)` lists `pid_start`. (Or just confirm `npm test` still exits 0 —
the migration runs on every startup.)

### Step 2: Capture the process start-time token at join

Add a helper, e.g. `processStartToken(pid)`, that reads `/proc/<pid>/stat` and returns field
22 (starttime) as a string — wrapped in try/catch returning `null` on any failure (non-Linux,
missing proc). Capture `processStartToken(process.pid)` in `registerAs` and store it in the
new `pid_start` column on INSERT/UPDATE (extend the existing `INSERT … ON CONFLICT` column
list — additive, keep all current columns).

**Verify**: after a join, the agent's row has a non-empty `pid_start` on this Linux box.

### Step 3: Verify identity in liveness, with safe fallback

Add a stronger check used by `onlineAgents()` (and reused where presence matters): an agent
is alive iff `pidAlive(pid)` **AND** (`pid_start` is null/unknown — backward-compat for rows
written before this change — OR the live process's current start token equals the stored
`pid_start`). If the PID is alive but the start token **differs**, it's a reused PID → treat
as dead and prune the row, exactly as `onlineAgents` prunes today.

Implement this as a small function (e.g. `agentAlive(row)`) and call it in `onlineAgents`
instead of the bare `pidAlive(a.pid)`. Keep `pidAlive` itself unchanged (other code/tests
may use it). Rows with `pid_start IS NULL` must still be treated as alive when their PID is
alive (don't regress existing sessions that joined before the column existed).

**Verify**: `node test/noise-and-presence.test.js` → exit 0; `npm test` → exit 0.

### Step 4: Add a PID-reuse regression test

Add a test that proves a reused PID is not seen as the same agent. Deterministic form against
a seeded DB: insert an `agents` row with `pid = process.pid` but a **bogus `pid_start`**
(e.g. `"999999999"` — a start time the current process does not have), then assert
`onlineAgents()` (or `agentAlive` on that row) treats it as **dead/pruned**, while a row with
the *correct* current start token (or `pid_start = NULL`) is treated as alive. Assert concrete
results (the bogus row is gone / excluded; the valid row is present).

**Verify**: `npm test` → exit 0; new check present.

## Test plan

- New test: row with live PID + mismatched `pid_start` → pruned/offline; row with live PID +
  matching-or-null `pid_start` → online.
- Existing presence tests (`noise-and-presence.test.js`, now wired in by plan 001) continue
  to pass.
- Model on `test/noise-and-presence.test.js`.

## Done criteria

ALL must hold:

- [ ] `agents` has a `pid_start` column (migration added; `PRAGMA table_info(agents)` shows it).
- [ ] Liveness used by `onlineAgents()` requires PID alive AND (start-token match OR stored
      token null); a live-PID-but-mismatched-token row is pruned.
- [ ] Backward-compat: rows with `pid_start IS NULL` are still treated as alive when the PID
      is alive (no regression for pre-existing sessions).
- [ ] `npm test` exits 0, including the new PID-reuse test.
- [ ] No files outside the in-scope list are modified (`git status --porcelain`).
- [ ] `plans/README.md` row for 004 updated (unless your reviewer maintains the index).

## STOP conditions

Stop and report (do not improvise) if:

- `server.js:95-102` (`pidAlive`) or `:172-180` (`onlineAgents`) does not match the "Current
  state" excerpts (drift).
- `/proc/<pid>/stat` is not readable in this environment (then `processStartToken` returns
  null everywhere and the hardening is a no-op — report this rather than shipping dead code;
  the reviewer may still want the column + null-safe path landed).
- The reuse test cannot be made deterministic without spawning/killing real processes and
  waiting on PID reuse (which is non-deterministic) — use the seeded-bogus-token approach in
  Step 4 instead; if even that is blocked, report.

## Maintenance notes

- `pid_start` is the per-process identity token; if presence logic moves into the shared
  module (see plan 002's `unread.js` for the shared-module pattern), bring `agentAlive` with
  it so `watch.js` and the watchers also stop trusting bare PIDs.
- Reviewer should confirm the backward-compat branch (`pid_start IS NULL → trust PID`) — a
  too-strict check here would make every pre-existing agent vanish from `who` on first run.
- Linux-only: the token read is `/proc`-based. If intercom is ever run on macOS, swap the
  token source (e.g. `ps -o lstart= -p <pid>`); the null-safe fallback keeps it correct
  meanwhile (degrades to today's PID-only behavior).
