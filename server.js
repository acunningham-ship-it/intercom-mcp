#!/usr/bin/env node
// intercom-mcp v2 — inter-session message bus for agents on one box.
//
// v2 adds: presence heartbeat (live/idle/stale status on who()), read-state
// (send shows recipient last_active; history shows per-message read receipts),
// topic routing (join with topics:[], send with topic: broadcasts to subscribers
// only), and inbox filters (from_agent / topic on inbox()).
//
// Each agent session spawns its own stdio instance of this server; all instances
// share one SQLite database (WAL mode). No daemon, no port. Delivery is durable:
// missed messages stay in the recipient's inbox until they pull them.

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { DatabaseSync } from "node:sqlite";
import { mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { basename, dirname, join as pathJoin } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { unreadFor as unreadForShared } from "./unread.js";

const DB_PATH =
  process.env.INTERCOM_DB ??
  pathJoin(homedir(), ".local", "share", "intercom", "intercom.db");

mkdirSync(dirname(DB_PATH), { recursive: true });

const db = new DatabaseSync(DB_PATH);
db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA busy_timeout = 5000;
  CREATE TABLE IF NOT EXISTS agents (
    name        TEXT PRIMARY KEY,
    pid         INTEGER NOT NULL,
    cwd         TEXT,
    role        TEXT,
    joined_at   TEXT NOT NULL,
    last_seen   TEXT NOT NULL,
    tmux_socket TEXT,
    tmux_pane   TEXT
  );
  CREATE TABLE IF NOT EXISTS messages (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    from_agent TEXT NOT NULL,
    to_agent   TEXT,
    kind       TEXT NOT NULL DEFAULT 'message',
    body       TEXT NOT NULL,
    reply_to   INTEGER,
    created_at TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_messages_to ON messages(to_agent, id);
  CREATE INDEX IF NOT EXISTS idx_messages_reply ON messages(reply_to);
  CREATE TABLE IF NOT EXISTS reads (
    agent      TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    PRIMARY KEY (agent, message_id)
  );
`);

// Additive migrations — guarded in try/catch so they're safe on existing databases.
// Never drop or rename columns; only ADD.
for (const col of [
  "tmux_socket TEXT",
  "tmux_pane TEXT",
  "topics TEXT",   // v2: JSON array of topic strings; NULL = receive all broadcasts
]) {
  try { db.exec(`ALTER TABLE agents ADD COLUMN ${col}`); } catch {}
}
for (const col of [
  "topic TEXT",    // v2: topic tag on a message; NULL = no topic
]) {
  try { db.exec(`ALTER TABLE messages ADD COLUMN ${col}`); } catch {}
}

// Retention: drop messages (and their read-receipts) older than N days.
const RETENTION_DAYS = Number(process.env.INTERCOM_RETENTION_DAYS ?? 7);
function pruneOld() {
  if (!(RETENTION_DAYS > 0)) return;
  const cutoff = new Date(Date.now() - RETENTION_DAYS * 86400000).toISOString();
  db.prepare("DELETE FROM messages WHERE created_at < ?").run(cutoff);
  db.exec("DELETE FROM reads WHERE message_id NOT IN (SELECT id FROM messages)");
}
pruneOld();

// ---------------------------------------------------------------------------
// identity & presence

let me = null;

const WAIT_SCRIPT = pathJoin(import.meta.dirname, "wait.js");
const MONITOR_SCRIPT = pathJoin(import.meta.dirname, "monitor-watcher.js");

const now = () => new Date().toISOString();

function pidAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return err.code === "EPERM";
  }
}

function ago(iso) {
  const s = Math.max(0, (Date.now() - Date.parse(iso)) / 1000);
  if (s < 60) return `${Math.round(s)}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  if (s < 86400) return `${Math.round(s / 3600)}h ago`;
  return `${Math.round(s / 86400)}d ago`;
}

// Presence status: live < 30s since last tool call, idle < 5m, stale >= 5m.
function statusLabel(iso) {
  const s = (Date.now() - Date.parse(iso)) / 1000;
  if (s < 30) return "live";
  if (s < 300) return "idle";
  return "stale";
}

function sanitizeName(name) {
  return name.trim().toLowerCase().replace(/[^a-z0-9_-]+/g, "-").slice(0, 48);
}

function registerAs(rawName, role, topics) {
  let name = sanitizeName(rawName);
  if (!name) name = `agent-${process.pid}`;
  // If the name is held by a different *live* process, suffix; a dead holder is replaced.
  const holder = db
    .prepare("SELECT pid FROM agents WHERE name = ?")
    .get(name);
  if (holder && holder.pid !== process.pid && pidAlive(holder.pid)) {
    let i = 2;
    while (true) {
      const candidate = `${name}-${i}`;
      const h = db.prepare("SELECT pid FROM agents WHERE name = ?").get(candidate);
      if (!h || h.pid === process.pid || !pidAlive(h.pid)) {
        name = candidate;
        break;
      }
      i++;
    }
  }
  const ts = now();
  // topics: null if not passed (COALESCE preserves existing); JSON string if provided.
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
  // Drop our previous name if we renamed mid-session.
  if (me && me !== name) {
    db.prepare("DELETE FROM agents WHERE name = ? AND pid = ?").run(me, process.pid);
  }
  me = name;
  return name;
}

function ensureJoined() {
  if (me) {
    db.prepare("UPDATE agents SET last_seen = ? WHERE name = ?").run(now(), me);
    return me;
  }
  return registerAs(`${basename(process.cwd()) || "agent"}-${process.pid % 10000}`);
}

function onlineAgents() {
  const rows = db.prepare("SELECT * FROM agents ORDER BY joined_at").all();
  const online = [];
  for (const a of rows) {
    if (pidAlive(a.pid)) online.push(a);
    else db.prepare("DELETE FROM agents WHERE name = ?").run(a.name);
  }
  return online;
}

// ---------------------------------------------------------------------------
// messages

function insertMessage({ to, kind, body, replyTo, topic }) {
  const res = db
    .prepare(
      `INSERT INTO messages (from_agent, to_agent, kind, body, reply_to, created_at, topic)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    )
    .run(me, to ?? null, kind, body, replyTo ?? null, now(), topic ?? null);
  return Number(res.lastInsertRowid);
}

// unreadFor: returns unread messages for `agent`, applying topic routing + optional filters.
//
// Topic routing: agents with topics set only receive topic-tagged broadcasts they subscribed
// to. Agents with topics=NULL (never called join with topics) receive ALL broadcasts
// (backward-compat). Directed messages bypass routing entirely.
//
// filters.from  — only show messages from this agent name
// filters.topic — only show messages tagged with this topic
function unreadFor(agent, { from, topic } = {}) {
  return unreadForShared(db, agent, { from, topic });
}

function markRead(agent, ids) {
  const stmt = db.prepare(
    "INSERT OR IGNORE INTO reads (agent, message_id) VALUES (?, ?)"
  );
  for (const id of ids) stmt.run(agent, id);
}

// ---------------------------------------------------------------------------
// formatting

function fmtMessage(m) {
  const to = m.to_agent ? `to ${m.to_agent}` : "broadcast";
  const topicTag = m.topic ? ` [topic:${m.topic}]` : "";
  let line = `[#${m.id}] ${m.from_agent} → ${to}${topicTag} (${m.kind}, ${ago(m.created_at)}): ${m.body}`;
  if (m.kind === "question") {
    line += `\n      ↳ answer with reply(message_id: ${m.id}, message: "...")`;
  }
  if (m.kind === "answer" && m.reply_to) {
    line = `[#${m.id}] ${m.from_agent} answered your question #${m.reply_to}${topicTag} (${ago(m.created_at)}): ${m.body}`;
  }
  return line;
}

const text = (s) => ({ content: [{ type: "text", text: s }] });

// ---------------------------------------------------------------------------
// server & tools

const server = new McpServer(
  { name: "intercom", version: "0.2.0" },
  {
    instructions: `Message bus between agent sessions (Claude Code, Codex, or any MCP client) running on this machine.
Start by calling join with a short descriptive name (e.g. the project you're working on).
- send: fire-and-forget message to one agent or broadcast to all. Add topic to route to subscribers only.
- ask: send a question and BLOCK until the recipient replies (or timeout). The question stays queued if they're busy.
- inbox: fetch your unread messages; pass wait_seconds to long-poll, from_agent/topic to filter.
- reply: answer a question or respond to a message by id.
- who: list online sessions with live/idle/stale presence status and last_active time.
Delivery: on join, arm a Monitor on monitor-watcher.js for clean event-based notifications (call inbox when it fires). Messages are never typed into your terminal. Delivery is durable: anything you miss is still in your inbox. If collaborating, also check inbox between tasks.`,
  }
);

server.tool(
  "join",
  "Join the intercom under a name so other Claude Code sessions can find and message you. Call this first. Re-call to rename or update topic subscriptions.",
  {
    name: z
      .string()
      .describe("Short kebab-case name, e.g. 'lead-engine' or 'reviewer'"),
    role: z
      .string()
      .optional()
      .describe("One line on what this session is working on"),
    topics: z
      .array(z.string())
      .optional()
      .describe(
        "Topic tags to subscribe to (e.g. ['alerts', 'rmi']). Omit to receive all broadcasts. " +
        "If set, you only receive topic-tagged broadcasts for your subscribed topics; non-topic broadcasts still reach you."
      ),
  },
  async ({ name, role, topics }) => {
    const assigned = registerAs(name, role, topics);
    const others = onlineAgents().filter((a) => a.name !== assigned);
    const unread = unreadFor(assigned).length;
    let out = `joined as "${assigned}"`;
    if (assigned !== sanitizeName(name)) out += ` (requested name was taken by a live session)`;
    out += others.length
      ? `\nonline now: ${others
          .map((a) => `${a.name}${a.role ? ` (${a.role})` : ""}`)
          .join(", ")}`
      : `\nno other sessions online yet.`;
    if (topics && topics.length) out += `\nsubscribed to topics: ${topics.join(", ")}`;
    if (unread) out += `\nyou have ${unread} unread message(s) — call inbox.`;
    out += `\nnotifications (recommended): arm a Monitor for clean message delivery —`;
    out += `\nMonitor(command="node ${MONITOR_SCRIPT} --me ${assigned}", persistent=true, description="intercom messages for ${assigned}")`;
    out += `\nwhen it fires, call inbox to read. This is the delivery path — messages never type into your terminal.`;
    out += `\nalternatively (no Monitor tool): start this once as a background shell —` +
      ` \`node ${WAIT_SCRIPT} --me ${assigned}\` — it prints a line when you have mail.`;
    return text(out);
  }
);

server.tool(
  "who",
  "List Claude Code sessions currently online on the intercom (name, role, cwd, last_active time, presence status: live/idle/stale).",
  {},
  async () => {
    ensureJoined();
    const agents = onlineAgents();
    if (!agents.length) return text("nobody online (not even you — this shouldn't happen)");
    const lines = agents.map((a) => {
      const status = statusLabel(a.last_seen);
      const topicsStr = a.topics ? ` topics:[${JSON.parse(a.topics).join(",")}]` : "";
      return `- ${a.name}${a.name === me ? " (you)" : ""}${a.role ? ` — ${a.role}` : ""} · cwd ${a.cwd} · last_active ${ago(a.last_seen)} [${status}]${topicsStr}`;
    });
    return text(lines.join("\n"));
  }
);

server.tool(
  "send",
  "Send a fire-and-forget message to another session (or broadcast to all). Add topic to route the broadcast only to subscribed agents.",
  {
    message: z.string().describe("The message body"),
    to: z
      .string()
      .optional()
      .describe("Recipient agent name from who(). Omit to broadcast to everyone."),
    topic: z
      .string()
      .optional()
      .describe(
        "Topic tag (e.g. 'alerts'). When broadcasting, only agents subscribed to this topic will receive it."
      ),
  },
  async ({ message, to, topic }) => {
    ensureJoined();
    if (to) {
      const known = onlineAgents().map((a) => a.name);
      if (!known.includes(to)) {
        return text(
          `no online agent named "${to}". online: ${known.filter((n) => n !== me).join(", ") || "(nobody else)"}.\nmessage NOT sent — retry with a valid name or omit 'to' to broadcast.`
        );
      }
    }
    const id = insertMessage({ to, kind: "message", body: message, topic });
    let out = `sent #${id} ${to ? `to ${to}` : "as broadcast"}${topic ? ` [topic:${topic}]` : ""}.`;
    if (to) {
      // Read-state: show recipient's last_active so sender knows if they're around.
      const recip = db.prepare("SELECT last_seen FROM agents WHERE name = ?").get(to);
      if (recip) {
        out += ` recipient last_active ${ago(recip.last_seen)} [${statusLabel(recip.last_seen)}].`;
      }
    } else if (topic) {
      out += ` only subscribers of "${topic}" will see this.`;
    }
    out += ` they'll get it via their monitor (or next inbox check).`;
    return text(out);
  }
);

server.tool(
  "ask",
  "Ask another session a question and wait for their answer (blocks up to timeout_seconds). If they don't reply in time, the question stays queued and you can check inbox later for the answer.",
  {
    question: z.string().describe("The question to ask"),
    to: z
      .string()
      .optional()
      .describe("Agent to ask. Omit to broadcast the question — first answer wins."),
    timeout_seconds: z
      .number()
      .int()
      .min(1)
      .max(240)
      .optional()
      .describe("How long to wait for an answer (default 60)"),
  },
  async ({ question, to, timeout_seconds }) => {
    ensureJoined();
    const timeout = (timeout_seconds ?? 60) * 1000;
    if (to) {
      const known = onlineAgents().map((a) => a.name);
      if (!known.includes(to)) {
        return text(
          `no online agent named "${to}". online: ${known.filter((n) => n !== me).join(", ") || "(nobody else)"}.\nquestion NOT sent.`
        );
      }
    }
    const qid = insertMessage({ to, kind: "question", body: question });
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
      const answer = db
        .prepare("SELECT * FROM messages WHERE kind = 'answer' AND reply_to = ? ORDER BY id LIMIT 1")
        .get(qid);
      if (answer) {
        markRead(me, [answer.id]);
        return text(`${answer.from_agent} answered (question #${qid}):\n${answer.body}`);
      }
      await sleep(400);
    }
    return text(
      `no answer after ${timeout / 1000}s. question #${qid} is still queued for ${to ?? "everyone"} — they'll see it on their next inbox check. check your inbox later for the answer (it will arrive as an 'answer' referencing #${qid}).`
    );
  }
);

server.tool(
  "reply",
  "Reply to a message or answer a question by its #id (shown in inbox). The reply is delivered to the original sender.",
  {
    message_id: z.number().int().describe("The #id of the message you're replying to"),
    message: z.string().describe("Your reply / answer"),
  },
  async ({ message_id, message }) => {
    ensureJoined();
    const orig = db.prepare("SELECT * FROM messages WHERE id = ?").get(message_id);
    if (!orig) return text(`no message #${message_id} exists.`);
    if (orig.from_agent === me) return text(`message #${message_id} is your own — nothing to reply to.`);
    const kind = orig.kind === "question" ? "answer" : "message";
    const id = insertMessage({
      to: orig.from_agent,
      kind,
      body: message,
      replyTo: message_id,
    });
    markRead(me, [message_id]);
    return text(
      `replied to #${message_id} (${orig.from_agent}) with #${id}.${
        kind === "answer" ? " if they're blocked on ask(), they get it within ~1s." : ""
      }`
    );
  }
);

server.tool(
  "inbox",
  "Fetch your unread messages (questions, answers, broadcasts). Pass wait_seconds to long-poll: blocks until a message arrives or the wait expires. Filter with from_agent or topic. Messages are marked read once fetched.",
  {
    wait_seconds: z
      .number()
      .int()
      .min(0)
      .max(240)
      .optional()
      .describe("If inbox is empty, wait up to this many seconds for something to arrive (default 0)"),
    from_agent: z
      .string()
      .optional()
      .describe("Only show messages from this agent name"),
    topic: z
      .string()
      .optional()
      .describe("Only show messages tagged with this topic"),
  },
  async ({ wait_seconds, from_agent, topic }) => {
    ensureJoined();
    const filters = { from: from_agent, topic };
    const deadline = Date.now() + (wait_seconds ?? 0) * 1000;
    let msgs = unreadFor(me, filters);
    while (!msgs.length && Date.now() < deadline) {
      await sleep(400);
      msgs = unreadFor(me, filters);
    }
    if (!msgs.length) {
      const filterDesc = [
        from_agent && `from:${from_agent}`,
        topic && `topic:${topic}`,
      ].filter(Boolean).join(", ");
      return text(
        wait_seconds
          ? `nothing arrived in ${wait_seconds}s${filterDesc ? ` (filter: ${filterDesc})` : ""}.`
          : `inbox empty${filterDesc ? ` (filter: ${filterDesc})` : ""}.`
      );
    }
    markRead(me, msgs.map((m) => m.id));
    const pendingQs = msgs.filter((m) => m.kind === "question").length;
    let out = `${msgs.length} message(s):\n` + msgs.map(fmtMessage).join("\n");
    if (pendingQs) out += `\n\n${pendingQs} question(s) above are waiting on you — reply promptly, the asker may be blocked.`;
    return text(out);
  }
);

server.tool(
  "history",
  "Show recent intercom traffic involving you (sent, received, broadcasts), newest last. Shows read-state for each message. Useful to recover context or re-read something already marked read.",
  {
    with: z
      .string()
      .optional()
      .describe("Only show traffic with this agent"),
    limit: z.number().int().min(1).max(200).optional().describe("Max messages (default 30)"),
  },
  async ({ with: withAgent, limit }) => {
    ensureJoined();
    let rows;
    if (withAgent) {
      rows = db
        .prepare(
          `SELECT * FROM messages
           WHERE (from_agent = ? AND (to_agent = ? OR to_agent IS NULL))
              OR (from_agent = ? AND (to_agent = ? OR to_agent IS NULL))
           ORDER BY id DESC LIMIT ?`
        )
        .all(me, withAgent, withAgent, me, limit ?? 30);
    } else {
      rows = db
        .prepare(
          `SELECT * FROM messages
           WHERE from_agent = ? OR to_agent = ? OR to_agent IS NULL
           ORDER BY id DESC LIMIT ?`
        )
        .all(me, me, limit ?? 30);
    }
    if (!rows.length) return text("no traffic yet.");

    // Annotate each message with read-state.
    const annotated = rows.reverse().map((m) => {
      let line = fmtMessage(m);
      if (m.from_agent === me) {
        // For messages I sent: show who has read them.
        const readBy = db
          .prepare("SELECT agent FROM reads WHERE message_id = ?")
          .all(m.id)
          .map((r) => r.agent);
        if (m.to_agent) {
          line += readBy.includes(m.to_agent) ? " ✓ read" : " · unread";
        } else if (readBy.length > 0) {
          line += ` · read by: ${readBy.join(", ")}`;
        }
      } else {
        // For messages I received: show whether I've read them.
        const iRead = db
          .prepare("SELECT 1 FROM reads WHERE agent = ? AND message_id = ?")
          .get(me, m.id);
        line += iRead ? " ✓ read" : " · unread";
      }
      return line;
    });

    return text(annotated.join("\n"));
  }
);

// ---------------------------------------------------------------------------

process.on("exit", () => {
  try {
    if (me) db.prepare("DELETE FROM agents WHERE name = ? AND pid = ?").run(me, process.pid);
    db.close();
  } catch {}
});

const transport = new StdioServerTransport();
await server.connect(transport);
