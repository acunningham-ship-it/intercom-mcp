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
| `join` | Register under a name (e.g. project name) + see who else is on. Call first. |
| `who` | List online sessions: name, role, cwd, last activity. |
| `send` | Fire-and-forget message to one agent, or broadcast (omit `to`). |
| `ask` | Send a question and **block** until answered (default 60s, max 240s). Times out gracefully — the question stays queued. |
| `reply` | Answer a question / respond to a message by `#id`. Unblocks a waiting `ask` within ~1s. |
| `inbox` | Fetch unread messages. `wait_seconds` long-polls until something arrives. |
| `history` | Re-read recent traffic (optionally filtered to one agent). |

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

Broadcasts are pull-only by design: they don't nudge every session, so a busy fleet
doesn't spam everyone. Directed messages are the ones that notify.

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
