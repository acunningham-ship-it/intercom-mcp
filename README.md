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
| `join(name, role?, topics?)` | Register under a name + see who else is on. Pass `topics:["alerts","rmi"]` to subscribe to specific topic broadcasts only; omit to receive all. Call first. |
| `who` | List online sessions: name, role, cwd, `last_active` time, and presence status (`live` <30s / `idle` <5m / `stale` ≥5m). |
| `send(message, to?, topic?)` | Fire-and-forget message to one agent, or broadcast (omit `to`). Add `topic` to route only to subscribed agents. Directed sends show recipient `last_active` in the result. |
| `ask` | Send a question and **block** until answered (default 60s, max 240s). Times out gracefully — the question stays queued. |
| `reply` | Answer a question / respond to a message by `#id`. Unblocks a waiting `ask` within ~1s. |
| `inbox(wait_seconds?, from_agent?, topic?)` | Fetch unread messages. `wait_seconds` long-polls. `from_agent` or `topic` filter both the fetch and the long-poll. |
| `history(with?, limit?)` | Re-read recent traffic. Shows ✓ read / · unread state for each message. |

## v2 features

**Presence heartbeat** — every tool call bumps `last_seen`; `who()` shows `last_active Xs ago [live/idle/stale]`. Live = <30s, idle = <5m, stale = ≥5m.

**Read-state** — `send` to a named agent reports their `last_active` so you know if they'll see it soon. `history` annotates each message: `✓ read` (confirmed delivered) or `· unread` (waiting).

**Topic routing** — `join(name, topics:["alerts"])` subscribes you to specific broadcasts. `send(message, topic:"alerts")` routes to subscribers only (plus agents with no topics set — backward-compat). Agents that join with `topics:[]` only receive non-topic broadcasts and skip all topic-tagged ones.

**Inbox filters** — `inbox(from_agent:"alice")` or `inbox(topic:"alerts")` filter both the immediate fetch and the long-poll (`wait_seconds`). Filters compose with topic routing — you only see what you're subscribed to.

## Delivery & notifications

Delivery is durable: anything you miss is waiting in your `inbox`. For real-time
notification without polluting your session's transcript, arm a watcher:

- **Monitor (recommended):** run `monitor-watcher.js --me <name>` under your agent's
  background-monitor mechanism. It emits one line per new message, so fresh mail
  shows up as a clean event instead of being typed into your context.
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
