# intercom-mcp

A coordination layer for AI agent fleets. An MCP server that lets agent sessions (Claude Code, Codex, or any MCP client) on one machine talk to each other: ask a question and block for the answer, fire-and-forget a message, broadcast status, and watch the whole fleet move in real time. No daemon, no port.

## How it works

No daemon, no port. Each agent session spawns its own stdio instance of
`server.js`; every instance reads/writes one shared SQLite database
(`~/.local/share/intercom/intercom.db`, WAL mode), so all sessions see the same
bus. Presence is pid-based: an agent is "online" while its server process is
alive, and dead rows are pruned automatically.

```
Claude session A ── server.js ─┐
Claude session B ── server.js ─┼─→  intercom.db (SQLite WAL)
Claude session C ── server.js ─┘
```

## Tools

| Tool | What it does |
|------|--------------|
| `join(name, role?, topics?, token?, memory_scope?, takeover?)` | **Sign in** under a name + see who else is on. Identities are durable: reuse the same name across reboots and your role/topics are restored. Pass `topics:["alerts","rmi"]` to subscribe to specific topic broadcasts only; omit to receive all. Call first. |
| `who(active_only?, include_offline?)` | List online sessions: name, role, cwd, `last_active` time, and presence status (`live` <30s / `idle` <5m / `stale` ≥5m). `include_offline:true` also lists durable identities that are currently offline. |
| `send(message, to?, topic?, type?, payload?, ttl_seconds?)` | Fire-and-forget message to one agent, or broadcast (omit `to`). Add `topic` to route only to subscribed agents. Add `type` + `payload` for a **structured** message (typed routing — recipients pull it with `inbox(type:…)`). Add `ttl_seconds` to expire it: after that it shows `[STALE]` and drops out of unread. A directed send to an **offline** identity queues and is delivered on their next sign-in. |
| `ask` | Send a question and **block** until answered (default 60s, max 240s). Times out gracefully — the question stays queued. |
| `ask_async(question, to?)` | Fire a question **without blocking** — returns its `#id` immediately. Fan out several, then `wait_for_any`. |
| `wait_for_any(question_ids, timeout_seconds?)` | Block until the **first** answer to any of the given question ids arrives. Pairs with `ask_async` for parallel-question coordination. |
| `reply` | Answer a question / respond to a message by `#id`. Unblocks a waiting `ask` within ~1s. |
| `inbox(wait_seconds?, from_agent?, topic?, type?)` | Fetch unread messages. `wait_seconds` long-polls. `from_agent`, `topic`, or `type` filter both the fetch and the long-poll. Expired (`ttl`) messages don't count as unread. |
| `history(with?, limit?)` | Re-read recent traffic. Shows ✓ read / · unread state for each message. |

## v3 — persistent identity

A row in the bus is a **durable identity**, not a live process. `join` is a sign-in:
a fresh process re-attaches to its existing identity and gets its `role`/`topics`/scope
back, instead of starting blank. Dead sessions go **offline** (still addressable —
messages queue for them) rather than being deleted; offline identities age out only
after `INTERCOM_IDENTITY_TTL_DAYS` (default 30) of silence.

**Reattach guardrail (not auth).** This is a single-user box, so nothing here is
cryptographic — it exists to catch a mistyped `join` name, not an adversary. An offline
identity may be reattached from its own boot scope (same `cwd` it was created in) or by
passing its `token` (issued at creation, stashed `0600` under `~/.intercom/agents/`).
A different session that grabs someone's offline name from a different cwd without the
token is **suffixed** (`judge` → `judge-2`) instead of silently impersonating them, and
every reattach is announced so the agent notices if it took the wrong name. A live
holder can only be seized with `takeover:true` + the credential. Identities created
before v3 stay open until their first reattach, which upgrades them with a scope + token.

## v2 features

**Presence heartbeat** — every tool call bumps `last_seen`; `who()` shows `last_active Xs ago [live/idle/stale]`. Live = <30s, idle = <5m, stale = ≥5m.

**Read-state** — `send` to a named agent reports their `last_active` so you know if they'll see it soon. `history` annotates each message: `✓ read` (confirmed delivered) or `· unread` (waiting).

**Topic routing** — `join(name, topics:["alerts"])` subscribes you to specific broadcasts. `send(message, topic:"alerts")` routes to subscribers only (plus agents with no topics set — backward-compat). Agents that join with `topics:[]` only receive non-topic broadcasts and skip all topic-tagged ones.

**Inbox filters** — `inbox(from_agent:"alice")` or `inbox(topic:"alerts")` filter both the immediate fetch and the long-poll (`wait_seconds`). Filters compose with topic routing — you only see what you're subscribed to.

## Delivery & notifications

Delivery is durable: anything you miss is waiting in your `inbox`. For real-time
notification without polluting your session's transcript, arm a watcher:

- **Monitor (recommended):** run `monitor-watcher.js --server-pid <pid> --me <name>`
  under your agent's background-monitor mechanism (`join` prints the exact command).
  It emits one line per new message, naming the identity it watches, so fresh mail
  shows up as a clean event instead of being typed into your context.
  `--server-pid` binds the watcher to your SESSION rather than to a name string: the
  identity is re-resolved from the server every poll, so if you rename mid-session the
  watcher follows instead of silently polling an inbox you no longer own. `--me` alone
  still works and stays pinned to that name. One watcher per identity — arming a new
  one retires the stale holder.
- **Status line:** `claude-code-statusline.js` puts an unread count in the status
  bar, fully out of the way.
- **Pull / blocking:** `inbox` long-polls (`wait_seconds`), and `ask` already blocks
  until the other side `reply`s (~1s), so request/response needs no watcher at all.

No message is ever typed into a session's terminal. Both directed and broadcast
messages are surfaced the same way — through the recipient's Monitor/watcher or
their next `inbox` check — so a busy fleet never gets keystrokes injected into a
live session. (An earlier version send-keys'd a nudge into tmux panes; that was
removed once monitors handled delivery.)

## Watch the fleet

`node watch.js` is a live, read-only dashboard of the bus: who's online, the recent
message flow, and which questions are still open. It's how you actually see a fleet of
agents coordinate. Add `--follow` to stream it. Example output:

```
┌─ Intercom Fleet Status
├─ Online Agents (4)
│   boss  ·  worker-2  ·  worker-3  ·  worker-4
├─ Open Questions (1)
│   #57 worker-2 → boss: "which lane owns the config file?"
├─ Recent Message Flow
│   #58 boss → worker-2   "take the parser lane, file-disjoint from w3"
│   #59 worker-3 → boss   "lane done, 12/12 green"
└─ (read-only; ^C to exit)
```

## Ops

- Register it with any MCP client by pointing the client at `node /path/to/intercom-mcp/server.js` (stdio). Use an absolute `node` path so it doesn't depend on `PATH`. Every session that registers shares the same bus.
- Test: `npm test` (spawns 3 real server processes on a temp DB; no API key needed).
- Override the DB path: `INTERCOM_DB=/path/to.db`.
- Retention: messages + read-receipts older than `INTERCOM_RETENTION_DAYS` (default 7) are pruned on `join`; set `0` to disable.
- Needs node >= 22.5 (for `node:sqlite`).
- Waits are capped at 240s to stay under MCP tool timeouts; raise `MCP_TOOL_TIMEOUT` if you push the caps.
