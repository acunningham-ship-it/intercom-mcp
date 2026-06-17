# Plan 003: Make the `join` name-claim atomic (close the TOCTOU)

> **Executor instructions**: Follow this plan step by step. Run every verification
> command and confirm the expected result before moving to the next step. If anything
> in the "STOP conditions" section occurs, stop and report — do not improvise. When
> done, update the status row for this plan in `plans/README.md` — unless a reviewer
> dispatched you and told you they maintain the index.
>
> **Drift check (run first)**: `git diff --stat 53b4bd5..HEAD -- server.js`
> If `server.js` changed since this plan was written, compare the "Current state" excerpt
> against the live code before proceeding; on a mismatch, treat it as a STOP condition.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: MED
- **Depends on**: plans/001 (DONE — its name-collision test guards this)
- **Category**: bug (concurrency)
- **Planned at**: commit `53b4bd5`, 2026-06-17

## Why this matters

`registerAs` claims a name with a **check-then-act** sequence that is not atomic: it reads
the current holder of a name, decides the name is free (or picks a suffix), then does a
separate `INSERT … ON CONFLICT DO UPDATE`. Two sessions joining the same name at the same
moment can both pass the check and both write — and because the conflict clause overwrites
`pid` with the latest writer, **the agents row ends up pointing at whichever process wrote
last**. The other live process still believes it owns the name (`me` is set in its memory)
but directed messages to that name now route to the other process, so it silently stops
receiving. On a fleet that spins up workers concurrently (the exact use case), same-name
collisions are plausible.

## Current state

`server.js:124-162`:

```js
function registerAs(rawName, role, topics) {
  let name = sanitizeName(rawName);
  if (!name) name = `agent-${process.pid}`;
  // If the name is held by a different *live* process, suffix; a dead holder is replaced.
  const holder = db.prepare("SELECT pid FROM agents WHERE name = ?").get(name);
  if (holder && holder.pid !== process.pid && pidAlive(holder.pid)) {
    let i = 2;
    while (true) {
      const candidate = `${name}-${i}`;
      const h = db.prepare("SELECT pid FROM agents WHERE name = ?").get(candidate);
      if (!h || h.pid === process.pid || !pidAlive(h.pid)) { name = candidate; break; }
      i++;
    }
  }
  const ts = now();
  const topicsJson = Array.isArray(topics) ? JSON.stringify(topics) : null;
  db.prepare(
    `INSERT INTO agents (name, pid, cwd, role, joined_at, last_seen, tmux_socket, tmux_pane, topics)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(name) DO UPDATE SET
       pid = excluded.pid, cwd = excluded.cwd,
       role = COALESCE(excluded.role, agents.role),
       last_seen = excluded.last_seen,
       tmux_socket = excluded.tmux_socket, tmux_pane = excluded.tmux_pane,
       topics = COALESCE(excluded.topics, agents.topics)`
  ).run(name, process.pid, process.cwd(), role ?? null, ts, ts, null, null, topicsJson);
  if (me && me !== name) {
    db.prepare("DELETE FROM agents WHERE name = ? AND pid = ?").run(me, process.pid);
  }
  me = name;
  return name;
}
```

Relevant facts:
- `agents.name` is the PRIMARY KEY (`server.js:32-41`), so the DB can enforce uniqueness — the
  bug is that the *claim* doesn't use that enforcement atomically.
- DB is SQLite via `node:sqlite` `DatabaseSync` with `PRAGMA busy_timeout = 5000` and WAL.
  `node:sqlite` supports `db.exec("BEGIN IMMEDIATE")` / `COMMIT` / `ROLLBACK`.
- A re-join by the **same** process (same pid) updating its own row is normal and must keep
  working (the `holder.pid === process.pid` and `h.pid === process.pid` checks above allow it).
- Helpers available: `sanitizeName` (`server.js:120`), `pidAlive` (`server.js:95`), `now`
  (`server.js:93`).

## Commands you will need

| Purpose      | Command                          | Expected on success      |
|--------------|----------------------------------|--------------------------|
| Install deps | `npm install`                    | exit 0                   |
| Full suite   | `npm test`                       | exit 0, all green        |
| Name test    | `node test/v3-features.test.js`  | exit 0                   |

Runtime requires node >= 22.5.

## Scope

**In scope**:
- `server.js` — `registerAs` only (and a small transaction helper if you add one nearby).
- `test/v3-features.test.js` (or a new `test/name-claim.test.js`) — concurrency/collision test.

**Out of scope**:
- The `agents` table schema — do NOT alter columns. The PRIMARY KEY already gives you
  atomicity to build on.
- `ensureJoined` (`server.js:164-170`), `onlineAgents` (`server.js:172-180`) — unchanged.
- Any other tool body.

## Git workflow

- Branch: `advisor/003-atomic-name-claim`
- Conventional commits (e.g. `fix(intercom): claim join name atomically to close TOCTOU`).
- Do NOT push or open a PR.

## Steps

### Step 1: Wrap the claim in an IMMEDIATE transaction with retry on the unique constraint

Restructure `registerAs` so the **find-free-name + insert** happens inside a single
`BEGIN IMMEDIATE` … `COMMIT` transaction (a write transaction acquires the write lock up
front, serializing concurrent claimers). Within the transaction:

- Re-read the holder, pick the name/suffix exactly as today (preserve the same-pid re-join
  and dead-holder-replacement behavior).
- Insert. If a concurrent claimer still wins the race and the `INSERT` would collide on a
  name held by a *different live* pid, catch the constraint error, `ROLLBACK`, bump the
  suffix, and retry — bounded (e.g. up to ~50 attempts, then fall back to
  `${name}-${process.pid}` which is process-unique). Do NOT loop unbounded.

The ON CONFLICT clause must remain a legitimate path **only** for the same-process re-join
and dead-holder replacement (updating an existing row the caller is entitled to). A *live
different-pid* holder must never be overwritten — that's the bug. Encode that explicitly
(e.g. only `DO UPDATE` when `agents.pid = excluded.pid OR <holder pid not alive>`; otherwise
treat as a collision and suffix). `node:sqlite` `ON CONFLICT … DO UPDATE … WHERE <cond>`
with a `DO NOTHING`/constraint fallback is acceptable, or do an explicit `SELECT` +
conditional `INSERT`/`UPDATE` inside the transaction — either is fine as long as the whole
sequence is inside `BEGIN IMMEDIATE`/`COMMIT` and a live different-pid holder is never
clobbered.

**Verify**: `node test/v3-features.test.js` → exit 0 (existing join/collision behavior intact).

### Step 2: Add a concurrent-claim test

Add a test that simulates two sessions claiming the same name. Simplest deterministic form
against a seeded DB: insert an `agents` row for name `worker` with a **live** pid
(use `process.pid` — guaranteed alive), then call the claim path for a *second* identity
requesting `worker`, and assert it gets `worker-2` and that the original `worker` row's pid
is **unchanged** (not clobbered). If the harness can spawn two real server processes racing
on `join("worker")`, even better — assert exactly one ends up named `worker` and the other
`worker-2`, and both appear in `who`. Assert concrete names.

**Verify**: `npm test` → exit 0; new check present.

### Step 3: Confirm same-process re-join still works

Re-joining with the same process (e.g. to add `topics` or change `role`) must still update
the existing row in place, not create `name-2`. The existing tests in `v3-features.test.js`
("join without topics works", topic re-subscribe) cover much of this — confirm they pass.

**Verify**: `npm test` → exit 0.

## Test plan

- New test: a live different-pid holder of a name is never overwritten; the second claimer
  is suffixed; the original row's pid is preserved.
- Existing re-join tests continue to pass (same-pid update in place).
- Model on `test/v3-features.test.js`.

## Done criteria

ALL must hold:

- [ ] The name-claim find+insert is inside a single `BEGIN IMMEDIATE`/`COMMIT` transaction.
- [ ] A live different-pid holder of a name is never overwritten (proven by the new test).
- [ ] Suffix retry is bounded (no unbounded `while (true)` that can spin without progress).
- [ ] `npm test` exits 0, including the new concurrent-claim test.
- [ ] No files outside the in-scope list are modified (`git status --porcelain`).
- [ ] `plans/README.md` row for 003 updated (unless your reviewer maintains the index).

## STOP conditions

Stop and report (do not improvise) if:

- The `server.js:124-162` `registerAs` body does not match the "Current state" excerpt (drift).
- `node:sqlite` in this runtime does not support `BEGIN IMMEDIATE`/explicit transactions the
  way the plan assumes (verify with a tiny probe; if it genuinely can't, report — do not
  invent a lock file or external mutex).
- Making the claim atomic breaks an existing re-join test in a way that suggests the current
  same-pid update semantics are intentional and incompatible with the new approach.

## Maintenance notes

- `registerAs` is the only writer of `agents.name`; keeping the claim atomic here keeps the
  invariant "one live owner per name" true. If a future feature lets an agent rename, route
  it through this same atomic path.
- Reviewer should scrutinize: the same-pid re-join path (must still update in place), the
  bounded-retry fallback, and that `BEGIN IMMEDIATE` doesn't deadlock with the `pruneOld`
  delete on `join` (they run in the same process sequentially — confirm ordering).
