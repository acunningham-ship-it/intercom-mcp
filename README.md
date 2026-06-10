# intercom-mcp

Message bus that lets agent sessions (Claude Code, Codex, or any MCP client) on the same
machine talk to each other — ask questions, answer them, broadcast status.

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

## Waking idle sessions

Delivery is durable — anything you miss is in your `inbox` — but a session also
gets **woken** when it can be, by either of two paths (`join` tells each session
which one applies):

- **In tmux (zero setup):** each session records its `$TMUX` pane on `join`, so
  when someone `send`s/`ask`s you, your intercom process types a one-line nudge
  into your pane via `tmux send-keys`. An idle session wakes and checks inbox.
  Verified end-to-end.
- **Not in tmux:** run the watcher as a background shell or monitor —
  `node wait.js --me <name>` — it prints one line and exits when you have mail,
  which your harness turns into a wake (re-invokes the idle session to call
  `inbox`). A child MCP can't push input into a non-tmux parent (OS limit), so
  the session listens instead of being pushed to.

For pure request/response, `ask` already blocks until the other side `reply`s
(within ~1s), so no waking is needed there.

## Ops

- Register it with any MCP client by pointing the client at `node /path/to/intercom-mcp/server.js` (stdio). Use an absolute `node` path so it doesn't depend on `PATH`. Every session that registers shares the same bus.
- Test: `npm test` (spawns 3 real server processes on a temp DB; no API key needed).
- Override the DB path: `INTERCOM_DB=/path/to.db`.
- Retention: messages + read-receipts older than `INTERCOM_RETENTION_DAYS` (default 7) are pruned on `join`; set `0` to disable.
- Needs node >= 22.5 (for `node:sqlite`).
- Waits are capped at 240s to stay under MCP tool timeouts; raise `MCP_TOOL_TIMEOUT` if you push the caps.
