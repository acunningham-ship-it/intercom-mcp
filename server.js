#!/usr/bin/env node
// intercom-mcp — inter-session message bus for agents on one box.
//
// Each agent session (Claude Code, Codex, or any MCP client) spawns its own instance of
// this stdio server; all instances share one SQLite database (WAL), so there is
// no daemon and no port. Agents join with a name, then send/ask/reply/inbox
// against the shared bus.
//
// Waking idle sessions: if a session runs inside tmux (it records its pane on
// join), messaging it will type a one-line nudge into that pane via send-keys —
// so an idle recipient wakes and checks its inbox instead of relying on polling.
// No tmux → graceful fallback to pull (they see it on their next inbox check).

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { DatabaseSync } from "node:sqlite";
import { mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { basename, dirname, join as pathJoin } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { execFileSync } from "node:child_process";

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

// Additive migrations for databases created before these columns existed.
for (const col of ["tmux_socket TEXT", "tmux_pane TEXT"]) {
  try {
    db.exec(`ALTER TABLE agents ADD COLUMN ${col}`);
  } catch {
    // column already present — ignore
  }
}

// Retention: drop messages (and their read-receipts) older than N days so the
// bus database can't grow without bound. INTERCOM_RETENTION_DAYS=0 disables it.
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

let me = null; // this session's agent name

// tmux coordinates of THIS session, inherited from the pane that launched us.
// Present only when running inside tmux; lets peers wake us via send-keys.
const TMUX_SOCKET = (process.env.TMUX || "").split(",")[0] || null;
const TMUX_PANE = process.env.TMUX_PANE || null;

// The tmux-free wake path: a background watcher the session arms on join.
const WAIT_SCRIPT = pathJoin(import.meta.dirname, "wait.js");

// Clean event-based notifications: Monitor script for message delivery.
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

function sanitizeName(name) {
  return name.trim().toLowerCase().replace(/[^a-z0-9_-]+/g, "-").slice(0, 48);
}

function registerAs(rawName, role) {
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
  db.prepare(
    `INSERT INTO agents (name, pid, cwd, role, joined_at, last_seen, tmux_socket, tmux_pane)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(name) DO UPDATE SET
       pid = excluded.pid, cwd = excluded.cwd,
       role = COALESCE(excluded.role, agents.role),
       last_seen = excluded.last_seen,
       tmux_socket = excluded.tmux_socket, tmux_pane = excluded.tmux_pane`
  ).run(name, process.pid, process.cwd(), role ?? null, ts, ts, TMUX_SOCKET, TMUX_PANE);
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

function insertMessage({ to, kind, body, replyTo }) {
  const res = db
    .prepare(
      `INSERT INTO messages (from_agent, to_agent, kind, body, reply_to, created_at)
       VALUES (?, ?, ?, ?, ?, ?)`
    )
    .run(me, to ?? null, kind, body, replyTo ?? null, now());
  return Number(res.lastInsertRowid);
}

function unreadFor(agent) {
  return db
    .prepare(
      `SELECT m.* FROM messages m
       WHERE m.from_agent != ?
         AND (m.to_agent = ? OR m.to_agent IS NULL)
         AND NOT EXISTS (SELECT 1 FROM reads r WHERE r.agent = ? AND r.message_id = m.id)
       ORDER BY m.id`
    )
    .all(agent, agent, agent);
}

function markRead(agent, ids) {
  const stmt = db.prepare(
    "INSERT OR IGNORE INTO reads (agent, message_id) VALUES (?, ?)"
  );
  for (const id of ids) stmt.run(agent, id);
}

// ---------------------------------------------------------------------------
// waking idle sessions
//
// If the target agent recorded a tmux pane on join, type a one-line nudge into
// that pane via send-keys so an idle session wakes and checks its inbox. The
// nudge is a fixed string (only the sanitized sender name is interpolated) — the
// message body is never injected, so a pane can't be fed arbitrary keystrokes.
// Best-effort: any failure (no tmux, dead pane, not in tmux) is swallowed and we
// fall back to pull delivery (the recipient's next inbox call still sees it).

function wake(targetName) {
  try {
    const a = db
      .prepare("SELECT pid, tmux_socket, tmux_pane FROM agents WHERE name = ?")
      .get(targetName);
    if (!a || !a.tmux_socket || !a.tmux_pane || !pidAlive(a.pid)) return false;
    const nudge = `[intercom] new message from ${me} — call the intercom "inbox" tool to read it.`;
    const base = ["-S", a.tmux_socket, "send-keys", "-t", a.tmux_pane];
    execFileSync("tmux", [...base, "-l", nudge], { timeout: 3000 });
    execFileSync("tmux", [...base, "Enter"], { timeout: 3000 });
    return true;
  } catch {
    return false;
  }
}

// Wake every online agent except the sender; returns how many were nudged.
// Wake other agents only for DIRECTED sends, not broadcasts.
// Broadcasts are pull-only to avoid mass-injecting nudge lines into every pane.
// Returns how many were nudged.
function wakeDirected(targetName) {
  return wake(targetName) ? 1 : 0;
}

function fmtMessage(m) {
  const to = m.to_agent ? `to ${m.to_agent}` : "broadcast";
  let line = `[#${m.id}] ${m.from_agent} → ${to} (${m.kind}, ${ago(m.created_at)}): ${m.body}`;
  if (m.kind === "question") {
    line += `\n      ↳ answer with reply(message_id: ${m.id}, message: "...")`;
  }
  if (m.kind === "answer" && m.reply_to) {
    line = `[#${m.id}] ${m.from_agent} answered your question #${m.reply_to} (${ago(m.created_at)}): ${m.body}`;
  }
  return line;
}

const text = (s) => ({ content: [{ type: "text", text: s }] });

// ---------------------------------------------------------------------------
// server & tools

const server = new McpServer(
  { name: "intercom", version: "0.1.0" },
  {
    instructions: `Message bus between agent sessions (Claude Code, Codex, or any MCP client) running on this machine.
Start by calling join with a short descriptive name (e.g. the project you're working on).
- send: fire-and-forget message to one agent or broadcast to all.
- ask: send a question and BLOCK until the recipient replies (or timeout). The question stays queued if they're busy.
- inbox: fetch your unread messages; pass wait_seconds to long-poll for new ones.
- reply: answer a question or respond to a message by id.
Waking: on join, arm a Monitor for clean event-based notifications (call inbox when it fires). For tmux sessions, messaging wakes you instantly. Delivery is durable either way: anything you miss is still in your inbox. If collaborating, also check inbox between tasks.`,
  }
);

server.tool(
  "join",
  "Join the intercom under a name so other Claude Code sessions can find and message you. Call this first. Re-call to rename.",
  {
    name: z
      .string()
      .describe("Short kebab-case name, e.g. 'lead-engine' or 'reviewer'"),
    role: z
      .string()
      .optional()
      .describe("One line on what this session is working on"),
  },
  async ({ name, role }) => {
    const assigned = registerAs(name, role);
    const others = onlineAgents().filter((a) => a.name !== assigned);
    const unread = unreadFor(assigned).length;
    let out = `joined as "${assigned}"`;
    if (assigned !== sanitizeName(name)) out += ` (requested name was taken by a live session)`;
    out += others.length
      ? `\nonline now: ${others
          .map((a) => `${a.name}${a.role ? ` (${a.role})` : ""}`)
          .join(", ")}`
      : `\nno other sessions online yet.`;
    if (unread) out += `\nyou have ${unread} unread message(s) — call inbox.`;
    // Recommend the Monitor for clean event-based notifications
    out += `\nnotifications (recommended): arm a Monitor for clean message delivery —`;
    out += `\nMonitor(command="node ${MONITOR_SCRIPT} --me ${assigned}", persistent=true, description="intercom messages for ${assigned}")`;
    out += `\nwhen it fires, call inbox to read. This gives you event-based notifications instead of pane injection.`;
    out += TMUX_PANE
      ? `\nalternatively (no setup): you're in tmux — peers can wake you instantly (no further setup needed).`
      : `\nalternatively (no monitor): to be nudged while idle, start this once as a background shell —` +
        ` \`node ${WAIT_SCRIPT} --me ${assigned}\` — it prints a line when you have mail.`;
    return text(out);
  }
);

server.tool(
  "who",
  "List Claude Code sessions currently online on the intercom (name, role, working dir, last activity).",
  {},
  async () => {
    ensureJoined();
    const agents = onlineAgents();
    if (!agents.length) return text("nobody online (not even you — this shouldn't happen)");
    const lines = agents.map(
      (a) =>
        `- ${a.name}${a.name === me ? " (you)" : ""}${a.role ? ` — ${a.role}` : ""} · cwd ${a.cwd} · active ${ago(a.last_seen)}`
    );
    return text(lines.join("\n"));
  }
);

server.tool(
  "send",
  "Send a fire-and-forget message to another session (or broadcast to all). The recipient sees it next time they check their inbox.",
  {
    message: z.string().describe("The message body"),
    to: z
      .string()
      .optional()
      .describe("Recipient agent name from who(). Omit to broadcast to everyone."),
  },
  async ({ message, to }) => {
    ensureJoined();
    if (to) {
      const known = onlineAgents().map((a) => a.name);
      if (!known.includes(to)) {
        return text(
          `no online agent named "${to}". online: ${known.filter((n) => n !== me).join(", ") || "(nobody else)"}.\nmessage NOT sent — retry with a valid name or omit 'to' to broadcast.`
        );
      }
    }
    const id = insertMessage({ to, kind: "message", body: message });
    // Directed sends nudge the recipient; broadcasts are pull-only to avoid noise.
    const woke = to ? wakeDirected(to) : 0;
    return text(
      `sent #${id} ${to ? `to ${to}` : "as broadcast"}.` +
        (woke
          ? ` nudged ${woke} session(s) to check inbox.`
          : ` they'll see it on their next inbox check.`)
    );
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
    // Directed questions nudge the recipient; broadcast questions are pull-only.
    if (to) wake(to);
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
    const woke = wake(orig.from_agent);
    return text(
      `replied to #${message_id} (${orig.from_agent}) with #${id}.${
        kind === "answer" ? " if they're blocked on ask(), they get it within ~1s." : ""
      }${woke ? " nudged their session to check inbox." : ""}`
    );
  }
);

server.tool(
  "inbox",
  "Fetch your unread messages (questions, answers, broadcasts). Pass wait_seconds to long-poll: blocks until a message arrives or the wait expires. Messages are marked read once fetched.",
  {
    wait_seconds: z
      .number()
      .int()
      .min(0)
      .max(240)
      .optional()
      .describe("If inbox is empty, wait up to this many seconds for something to arrive (default 0)"),
  },
  async ({ wait_seconds }) => {
    ensureJoined();
    const deadline = Date.now() + (wait_seconds ?? 0) * 1000;
    let msgs = unreadFor(me);
    while (!msgs.length && Date.now() < deadline) {
      await sleep(400);
      msgs = unreadFor(me);
    }
    if (!msgs.length) {
      return text(
        wait_seconds
          ? `nothing arrived in ${wait_seconds}s.`
          : "inbox empty."
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
  "Show recent intercom traffic involving you (sent, received, broadcasts), newest last. Useful to recover context or re-read something already marked read.",
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
    return text(rows.reverse().map(fmtMessage).join("\n"));
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
