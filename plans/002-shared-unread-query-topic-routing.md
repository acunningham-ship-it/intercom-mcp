# Plan 002: Extract the shared "unread for me" query (with topic routing) so the watchers match the server

> **Executor instructions**: Follow this plan step by step. Run every verification
> command and confirm the expected result before moving to the next step. If anything
> in the "STOP conditions" section occurs, stop and report — do not improvise. When
> done, update the status row for this plan in `plans/README.md` — unless a reviewer
> dispatched you and told you they maintain the index.
>
> **Drift check (run first)**: `git diff --stat 53b4bd5..HEAD -- server.js wait.js monitor-watcher.js`
> If any in-scope file changed since this plan was written, compare the "Current state"
> excerpts against the live code before proceeding; on a mismatch, treat it as a STOP
> condition.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: plans/001 (must be DONE — the baseline tests catch regressions here)
- **Category**: bug / tech-debt
- **Planned at**: commit `53b4bd5`, 2026-06-17

## Why this matters

The "unread messages for me" SQL predicate is **copy-pasted in three files and has
drifted**. `server.js` applies *topic routing* on top of the base query (an agent that
subscribed to specific topics does not receive topic-tagged broadcasts for other topics).
The two notification watchers — `wait.js` and `monitor-watcher.js` — use the base query
**without** topic routing. Result: an agent that joined with a `topics:[...]` subscription
gets **woken by a watcher for a broadcast it isn't subscribed to**, calls `inbox`, and
finds nothing — the server filtered it out. That's a spurious wake and a confusing
"nothing arrived" against a watcher that just said there was mail. It erodes trust in the
whole notification path, which is the feature these files exist for.

The fix is to make all three read paths agree by extracting one shared function and having
the watchers call it. This removes the duplication (the root cause) and the divergence (the
symptom) together.

## Current state

Three copies of the unread predicate:

- `server.js:203-229` — `unreadFor(agent, {from, topic})`. Runs the base SQL **then**
  applies topic routing in JS:
  ```js
  function unreadFor(agent, { from, topic } = {}) {
    let sql = `SELECT m.* FROM messages m
      WHERE m.from_agent != ?
        AND (m.to_agent = ? OR m.to_agent IS NULL)
        AND NOT EXISTS (SELECT 1 FROM reads r WHERE r.agent = ? AND r.message_id = m.id)`;
    const params = [agent, agent, agent];
    if (from) { sql += ` AND m.from_agent = ?`; params.push(from); }
    if (topic) { sql += ` AND m.topic = ?`; params.push(topic); }
    sql += ` ORDER BY m.id`;
    let msgs = db.prepare(sql).all(...params);
    // Topic routing (JS layer):
    const agentRow = db.prepare("SELECT topics FROM agents WHERE name = ?").get(agent);
    const myTopics = agentRow?.topics ? JSON.parse(agentRow.topics) : null;
    if (myTopics !== null) {
      msgs = msgs.filter((m) => {
        if (m.to_agent || !m.topic) return true; // directed or non-topic broadcast: always
        return myTopics.includes(m.topic);        // topic broadcast: check subscription
      });
    }
    return msgs;
  }
  ```
- `wait.js:71-84` — `getUnread()`: base SQL only (selects `id, from_agent, to_agent`), **no
  topic routing**.
- `monitor-watcher.js:70-84` — `getUnread()`: base SQL only (selects `id, from_agent,
  to_agent, kind`), **no topic routing**.

Conventions:
- ES modules (`"type": "module"` in `package.json`), Node built-ins imported as `node:*`.
- Each entry file opens its own `DatabaseSync` handle (no shared connection across processes
  — they're separate processes sharing the DB file). So the shared function must accept a
  `db` handle as a parameter, not import one.
- Topic routing only filters when `agents.topics` is non-NULL. `topics IS NULL` (the common,
  backward-compatible case) means "see all broadcasts" — that path must be preserved exactly.

## Commands you will need

| Purpose      | Command                                | Expected on success            |
|--------------|----------------------------------------|--------------------------------|
| Install deps | `npm install`                          | exit 0                         |
| Full suite   | `npm test`                             | exit 0, all checks green       |
| Topic test   | `node test/v3-features.test.js`        | exit 0 (topic-routing tests)   |
| Watcher test | `node test/v2-watchers.test.js`        | exit 0                         |

Runtime requires node >= 22.5.

## Scope

**In scope** (the only files you may modify/create):
- `unread.js` (CREATE — the shared module)
- `server.js` (replace the body of `unreadFor` to delegate to the shared module)
- `wait.js` (replace `getUnread` to delegate)
- `monitor-watcher.js` (replace `getUnread` to delegate)
- `test/v2-watchers.test.js` OR a new `test/topic-watcher.test.js` (add the regression test — see Test plan)

**Out of scope** (do NOT touch):
- The SQL schema / migrations in `server.js:29-73` — no schema change is needed.
- `watch.js` — its dashboard query is a different shape (open-asks join); leave it.
- The MCP tool definitions / their descriptions in `server.js`.
- `package.json` test script (001 already wired the suite; only ADD a file there if you
  create a brand-new test file, matching 001's pattern).

## Git workflow

- Branch: `advisor/002-shared-unread-query`
- Conventional-commit messages (e.g. `refactor(intercom): single source of truth for unread query`,
  `fix(intercom): watchers honor topic subscriptions`).
- Do NOT push or open a PR.

## Steps

### Step 1: Create `unread.js` with one exported function

Create `unread.js` exporting a function that takes a `db` handle and the agent name and
returns the unread rows **with topic routing applied** — identical semantics to
`server.js`'s current `unreadFor`. Signature:

```js
// unread.js
// Single source of truth for "unread messages for `agent`", including topic routing.
// Takes a db handle (each process opens its own) so server + watchers all agree.
export function unreadFor(db, agent, { from, topic } = {}) {
  // ... base SQL (from_agent != agent, to_agent = agent OR NULL, NOT in reads),
  //     optional from/topic filters, ORDER BY m.id,
  //     then the SAME JS topic-routing filter currently in server.js:216-226.
}
```

It must `SELECT m.*` (full rows) so all callers can pick the columns they need. Preserve
the `topics IS NULL → see all` behavior exactly.

**Verify**: `node -e "import('./unread.js').then(m => console.log(typeof m.unreadFor))"` → prints `function`.

### Step 2: Delegate from `server.js`

Add `import { unreadFor as unreadForShared } from "./unread.js";` near the other imports
(top of `server.js`). Replace the body of the existing `unreadFor(agent, {from, topic})` so
it calls `unreadForShared(db, agent, { from, topic })` and returns the result. Keep the
existing local function name and call sites unchanged (it's called in `join`, `inbox`).

**Verify**: `node test/v3-features.test.js` → exit 0 (topic-routing behavior unchanged on the server side).

### Step 3: Delegate from `wait.js` and `monitor-watcher.js`

In each watcher, replace its local `getUnread()` body to call the shared function against
its own already-open `db` handle: `unreadFor(db, me)`. Import it
(`import { unreadFor } from "./unread.js";`). Each watcher then maps the returned rows to
the fields it prints (`wait.js` uses `id, from_agent, to_agent`; `monitor-watcher.js` uses
`id, from_agent, to_agent, kind`) — the full-row result supports both. Keep each watcher's
output line format byte-for-byte the same.

**Verify**: `node test/v2-watchers.test.js` → exit 0; `node test/watch.test.js` → exit 0.

### Step 4: Add the cross-path regression test

Add a test (in `test/v2-watchers.test.js` or a new `test/topic-watcher.test.js` modeled on
it) that asserts: an agent subscribed to `topics:["alerts"]` does **not** get a topic
broadcast for `topic:"other"` surfaced by the watcher path — i.e. the watcher's unread set
for that agent excludes the unsubscribed topic broadcast, matching what `inbox` returns.
The cleanest way: call the shared `unreadFor(db, agent)` directly with a seeded DB (subscribe
the agent via an `agents` row with `topics='["alerts"]'`, insert a `topic='other'` broadcast)
and assert it is excluded; and a `topic='alerts'` broadcast is included. Assert concrete
message ids/counts.

**Verify**: `npm test` → exit 0; the new check appears in output.

### Step 5: Confirm no remaining duplicate predicate

**Verify**:
- `npm test` → exit 0.
- `git status --porcelain` shows only in-scope files.
- The base SQL string (`NOT EXISTS (SELECT 1 FROM reads`) now appears in `unread.js` and is
  no longer hand-written in `wait.js` / `monitor-watcher.js` / `server.js`'s `unreadFor`
  (they delegate). `watch.js` is out of scope and may still contain its own different query —
  that is expected.

## Test plan

- New test: topic-subscribed agent excludes unsubscribed-topic broadcast via the shared
  `unreadFor`, includes subscribed-topic and non-topic broadcasts and directed messages.
- Model structure on `test/v2-watchers.test.js`.
- Verification: `npm test` → all pass including the new check and the existing v2/v3 topic tests.

## Done criteria

ALL must hold:

- [ ] `unread.js` exists and exports `unreadFor(db, agent, opts)`.
- [ ] `server.js`, `wait.js`, `monitor-watcher.js` all import and delegate to it (no
      hand-written `NOT EXISTS (SELECT 1 FROM reads` string remains in those three files —
      `grep -c "NOT EXISTS (SELECT 1 FROM reads" server.js wait.js monitor-watcher.js` → all 0).
- [ ] `npm test` exits 0, including the new cross-path regression test.
- [ ] Watcher output line formats are unchanged (the v2-watchers / watch tests still pass).
- [ ] No files outside the in-scope list are modified (`git status --porcelain`).
- [ ] `plans/README.md` row for 002 updated (unless your reviewer maintains the index).

## STOP conditions

Stop and report (do not improvise) if:

- The `server.js:203-229` `unreadFor` body does not match the "Current state" excerpt (drift).
- Making the watchers delegate breaks `test/v2-watchers.test.js` in a way that reveals the
  watchers depend on the *un-routed* behavior intentionally (check whether any existing test
  asserts a topic-subscribed agent IS woken for an unsubscribed topic — if so, the intended
  behavior is ambiguous; report it rather than choosing).
- A step's verification fails twice after a reasonable fix attempt.
- The fix appears to require a schema/migration change or touching `watch.js`.

## Maintenance notes

- `unread.js` is now the single source of truth for "what counts as unread, including topic
  routing." Any future change to routing (new message kinds, mute lists, priority) goes there
  once, and all read paths inherit it.
- A reviewer should confirm the watcher output strings are byte-identical (notification
  formats are a contract with the harness/Monitor parsing them) and that the `topics IS NULL`
  backward-compat path still returns all broadcasts.
