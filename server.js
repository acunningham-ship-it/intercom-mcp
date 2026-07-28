#!/usr/bin/env node
// intercom-mcp v3 — inter-session message bus for agents on one box.
//
// v2 added: presence heartbeat (live/idle/stale status on who()), read-state
// (send shows recipient last_active; history shows per-message read receipts),
// topic routing (join with topics:[], send with topic: broadcasts to subscribers
// only), and inbox filters (from_agent / topic on inbox()).
//
// v3 (R4) adds PERSISTENT IDENTITY. A row in `agents` is now a DURABLE identity —
// name, role, topics, memory_scope and a token that survive across reboots — not a
// live process. Liveness (pid/pid_start) only marks whether a session is currently
// attached. join() became sign-in: a fresh process re-attaches to its existing
// identity (role/topics restored) instead of starting blank. Dead rows are NOT
// deleted; they go offline (still addressable — messages queue) and age out only via
// pruneIdentities (INTERCOM_IDENTITY_TTL_DAYS, default 30).
//
// Impersonation guardrail — NOT auth. This is a single-user box; any of the user's
// own processes can read any token file, so nothing here is cryptographic. It exists
// to catch ACCIDENTS (a mistyped sign_in name), not adversaries: reattaching an
// offline identity is allowed from its own memory_scope (cwd) or with its token, and
// every reattach is ANNOUNCED so the agent notices if it grabbed the wrong name.
//
// Each agent session spawns its own stdio instance of this server; all instances
// share one SQLite database (WAL mode). No daemon, no port. Delivery is durable:
// missed messages stay in the recipient's inbox until they pull them.

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { DatabaseSync } from "node:sqlite";
import { mkdirSync, readFileSync, writeFileSync, chmodSync } from "node:fs";
import { randomBytes } from "node:crypto";
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
  "topics TEXT",    // v2: JSON array of topic strings; NULL = receive all broadcasts
  "pid_start TEXT", // v3: process start-time token from /proc/<pid>/stat field 22
  "status TEXT",    // v3 Lane B: short status string set via update_status (e.g. "working", "done")
  "memory_scope TEXT", // R4: boot dir this identity belongs to (its ambient credential)
  "token TEXT",        // R4: reattach token, minted at creation (accident-guardrail, not auth)
  "left_at TEXT",      // R4: when the current session detached; NULL = attached/never-left
]) {
  try { db.exec(`ALTER TABLE agents ADD COLUMN ${col}`); } catch {}
}
for (const col of [
  "topic TEXT",      // v2: topic tag on a message; NULL = no topic
  "max_claims INTEGER", // r2-a: if set, broadcast is claimable by up to N agents
  "type TEXT",       // R3: structured message type for typed routing (e.g. 'task','status','alert')
  "payload TEXT",    // R3: JSON payload string carried alongside a typed message
  "expires_at TEXT", // R3: ISO expiry; after it the message is [STALE] and drops out of unread
]) {
  try { db.exec(`ALTER TABLE messages ADD COLUMN ${col}`); } catch {}
}

// r2-a: claims table — one row per (message, agent) that successfully claimed.
db.exec(`
  CREATE TABLE IF NOT EXISTS claims (
    message_id  INTEGER NOT NULL,
    agent       TEXT NOT NULL,
    claimed_at  TEXT NOT NULL,
    slot        INTEGER NOT NULL,
    PRIMARY KEY (message_id, agent)
  );
  CREATE TABLE IF NOT EXISTS assignments (
    agent       TEXT PRIMARY KEY,
    label       TEXT NOT NULL,
    assigned_at TEXT NOT NULL
  );
`);

// FTS5 virtual table for full-text search over message bodies.
// content= and content_rowid= make this a "contentless" FTS5 table that syncs with the messages table.
// On startup, rebuild to ensure any backfilled rows are indexed.
try {
  db.exec(`
    CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
      body,
      content='messages',
      content_rowid='id'
    );
    INSERT INTO messages_fts(messages_fts) VALUES('rebuild');
  `);
} catch {}


// Retention: drop messages (and their read-receipts) older than N days.
const RETENTION_DAYS = Number(process.env.INTERCOM_RETENTION_DAYS ?? 7);
function pruneOld() {
  if (!(RETENTION_DAYS > 0)) return;
  const cutoff = new Date(Date.now() - RETENTION_DAYS * 86400000).toISOString();
  db.prepare("DELETE FROM messages WHERE created_at < ?").run(cutoff);
  db.exec("DELETE FROM reads WHERE message_id NOT IN (SELECT id FROM messages)");
}
pruneOld();

// R4: age out durable identities that have been OFFLINE longer than the TTL, so
// one-off `<cwd>-<pid>` seats don't accumulate forever. Named roles that sign in
// within the window survive; a long-silent one re-mints cleanly on next sign-in.
const IDENTITY_TTL_DAYS = Number(process.env.INTERCOM_IDENTITY_TTL_DAYS ?? 30);
function pruneIdentities() {
  if (!(IDENTITY_TTL_DAYS > 0)) return;
  const cutoff = new Date(Date.now() - IDENTITY_TTL_DAYS * 86400000).toISOString();
  const rows = db.prepare("SELECT name, pid, pid_start, last_seen FROM agents WHERE last_seen < ?").all(cutoff);
  for (const r of rows) {
    if (!agentAlive(r)) db.prepare("DELETE FROM agents WHERE name = ?").run(r.name);
  }
}
pruneIdentities();

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

// Read process start-time from /proc/<pid>/stat field 22 (clock ticks since boot).
// Returns null on any failure (non-Linux, missing proc, bad format).
function processStartToken(pid) {
  try {
    const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
    // Field 22 is starttime; fields are space-separated but the process name
    // (field 2) may contain spaces inside parens — find the closing paren first.
    const closeParen = stat.lastIndexOf(")");
    if (closeParen === -1) return null;
    const fields = stat.slice(closeParen + 2).split(" ");
    return fields[19] ?? null; // 0-indexed: field 22 is index 19 after the paren
  } catch {
    return null;
  }
}

// An agent row is alive iff its PID is alive AND its start token matches (or
// it has no stored token — backward-compat for rows written before this change).
function agentAlive(row) {
  if (!pidAlive(row.pid)) return false;
  if (!row.pid_start) return true; // null/unknown → trust PID (backward-compat)
  const current = processStartToken(row.pid);
  if (current === null) return true; // can't read /proc → trust PID (safe fallback)
  return current === row.pid_start;
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

// R4 identity token: minted at creation, stashed 0600 under ~/.intercom/agents/.
// It's a convenience credential for the owner to reattach from a different cwd —
// NOT a secret (same-user processes can read the file). See the header note.
const TOKENS_DIR = pathJoin(homedir(), ".intercom", "agents");
const tokenPath = (name) => pathJoin(TOKENS_DIR, `${name}.token`);
const mintToken = () => randomBytes(9).toString("hex");
function writeTokenFile(name, token) {
  try {
    mkdirSync(TOKENS_DIR, { recursive: true });
    writeFileSync(tokenPath(name), token, { mode: 0o600 });
    chmodSync(tokenPath(name), 0o600);
  } catch {}
}

// Any row (online or offline) — a durable identity lookup.
const identityRow = (name) => db.prepare("SELECT * FROM agents WHERE name = ?").get(name);
// Every identity, newest-joined last. No mutation (unlike the old onlineAgents).
const allIdentities = () => db.prepare("SELECT * FROM agents ORDER BY joined_at").all();
// Currently-attached identities only. Non-destructive: offline rows are LEFT in place.
const onlineAgents = () => allIdentities().filter(agentAlive);

// The accident-guardrail (NOT auth — see header). An offline identity may be
// reattached from its own boot scope (cwd == memory_scope), or by explicitly
// passing its token. The token is NOT auto-read from the stashed file — that file
// is the owner's recovery copy to look up and pass, not an ambient credential
// (any same-user process could read it, which would make the guardrail theater).
// Legacy rows (pre-R4, no scope AND no token) stay open for backward-compat.
function credentialOk(row, cwd, token) {
  if (row.memory_scope && cwd === row.memory_scope) return true; // same boot scope
  if (row.token && token && token === row.token) return true;    // explicit token override
  if (!row.token && !row.memory_scope) return true;              // legacy backward-compat
  return false;
}

// sign_in: attach this process to the named identity, creating it if new.
// Returns { name, status: new|rejoin|reattach|takeover, token, restored }.
// role/topics/memory_scope/token/joined_at all persist across reboots.
function signIn(rawName, opts = {}) {
  const { role, topics, memoryScope, token, takeover } = opts;
  const baseName = sanitizeName(rawName) || `agent-${process.pid}`;
  const topicsJson = Array.isArray(topics) ? JSON.stringify(topics) : null;
  const pidStart = processStartToken(process.pid);
  const cwd = process.cwd();
  const scope = memoryScope ?? cwd;
  const MAX_RETRIES = 50;

  // Reattach/rejoin update — preserves the identity's durable fields.
  const REATTACH = `UPDATE agents SET pid = ?, pid_start = ?, cwd = ?, role = COALESCE(?, role),
       last_seen = ?, tmux_socket = NULL, tmux_pane = NULL,
       topics = COALESCE(?, topics), left_at = NULL WHERE name = ?`;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const candidate =
      attempt === 0            ? baseName
      : attempt < MAX_RETRIES ? `${baseName}-${attempt + 1}`
      :                         `${baseName}-${process.pid}`;
    let result;
    try {
      db.exec("BEGIN IMMEDIATE");
      const ts = now();
      const row = db.prepare("SELECT * FROM agents WHERE name = ?").get(candidate);

      if (!row) {
        // New identity — mint a token, record the boot scope.
        const newToken = mintToken();
        db.prepare(
          `INSERT INTO agents (name, pid, pid_start, cwd, role, joined_at, last_seen,
             tmux_socket, tmux_pane, topics, memory_scope, token, left_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, NULL)`
        ).run(candidate, process.pid, pidStart, cwd, role ?? null, ts, ts, topicsJson, scope, newToken);
        db.exec("COMMIT");
        writeTokenFile(candidate, newToken);
        result = { name: candidate, status: "new", token: newToken, restored: null };
      } else if (row.pid === process.pid) {
        // Same-process re-call (rename/update) — no credential needed.
        db.prepare(REATTACH).run(process.pid, pidStart, cwd, role ?? null, ts, topicsJson, candidate);
        db.exec("COMMIT");
        result = { name: candidate, status: "rejoin", token: row.token, restored: null };
      } else if (!agentAlive(row)) {
        // Offline identity — the reboot path. Gate on the accident-guardrail.
        if (!credentialOk(row, cwd, token) && !takeover) {
          db.exec("ROLLBACK");
          continue; // a different agent grabbing an offline name → suffix instead
        }
        db.prepare(REATTACH).run(process.pid, pidStart, cwd, role ?? null, ts, topicsJson, candidate);
        // Upgrade a legacy (pre-R4) identity in place: give it a scope + token now, so
        // identities that predate this feature gain the guardrail on their first reboot
        // instead of staying permanently open until the TTL prune.
        let outToken = row.token;
        if (!row.memory_scope && !row.token) {
          outToken = mintToken();
          db.prepare("UPDATE agents SET memory_scope = ?, token = ? WHERE name = ?").run(cwd, outToken, candidate);
        }
        db.exec("COMMIT");
        if (outToken !== row.token) writeTokenFile(candidate, outToken);
        result = { name: candidate, status: "reattach", token: outToken,
                   restored: { role: row.role, topics: row.topics },
                   upgraded: outToken !== row.token };
      } else {
        // Live different-pid holder — only takeover + credential may seize it.
        if (takeover && credentialOk(row, cwd, token)) {
          db.prepare(REATTACH).run(process.pid, pidStart, cwd, role ?? null, ts, topicsJson, candidate);
          db.exec("COMMIT");
          result = { name: candidate, status: "takeover", token: row.token,
                     restored: { role: row.role, topics: row.topics } };
        } else {
          db.exec("ROLLBACK");
          continue; // suffix and retry
        }
      }
    } catch (err) {
      try { db.exec("ROLLBACK"); } catch {}
      throw err;
    }

    // Renamed mid-session — mark our previous identity offline (durable, not deleted).
    if (me && me !== result.name) {
      db.prepare("UPDATE agents SET left_at = ? WHERE name = ? AND pid = ?").run(now(), me, process.pid);
    }
    me = result.name;
    return result;
  }

  throw new Error("signIn: exhausted all suffix attempts");
}

function ensureJoined() {
  if (me) {
    db.prepare("UPDATE agents SET last_seen = ?, left_at = NULL WHERE name = ?").run(now(), me);
    return me;
  }
  return signIn(`${basename(process.cwd()) || "agent"}-${process.pid % 10000}`).name;
}

// ---------------------------------------------------------------------------
// messages

function insertMessage({ to, kind, body, replyTo, topic, type, payload, expiresAt }) {
  const res = db
    .prepare(
      `INSERT INTO messages (from_agent, to_agent, kind, body, reply_to, created_at, topic, type, payload, expires_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .run(me, to ?? null, kind, body, replyTo ?? null, now(), topic ?? null,
         type ?? null,
         payload == null ? null : (typeof payload === "string" ? payload : JSON.stringify(payload)),
         expiresAt ?? null);

  const msgId = Number(res.lastInsertRowid);

  // Write-through to FTS5 table. Since we're using content= mode, this is optional
  // (the FTS table auto-syncs), but explicit insert ensures immediate searchability.
  try {
    db.prepare(
      `INSERT INTO messages_fts(rowid, body) VALUES (?, ?)`
    ).run(msgId, body);
  } catch {}

  return msgId;
}

// unreadFor: returns unread messages for `agent`, applying topic routing + optional filters.
//
// Topic routing: agents with topics set only receive topic-tagged broadcasts they subscribed
// to. Agents with topics=NULL (never called join with topics) receive ALL broadcasts
// (backward-compat). Directed messages bypass routing entirely.
//
// filters.from  — only show messages from this agent name
// filters.topic — only show messages tagged with this topic
function unreadFor(agent, { from, topic, type } = {}) {
  return unreadForShared(db, agent, { from, topic, type });
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
  const typeTag = m.type ? ` <${m.type}>` : "";
  const staleTag = m.expires_at && m.expires_at < now() ? " [STALE]" : "";
  const replyPrefix = m.reply_to ? `↩ #${m.reply_to} ` : "";
  let line = `[#${m.id}]${staleTag} ${m.from_agent} → ${to}${topicTag}${typeTag} (${m.kind}, ${ago(m.created_at)}): ${replyPrefix}${m.body}`;
  if (m.kind === "question") {
    line += `\n      ↳ answer with reply(message_id: ${m.id}, message: "...")`;
  }
  if (m.kind === "answer" && m.reply_to) {
    // For answers, use the special format but preserve reply_to prefix if present.
    line = `[#${m.id}]${staleTag} ${m.from_agent} answered your question ${replyPrefix}#${m.reply_to}${topicTag}${typeTag} (${ago(m.created_at)}): ${m.body}`;
  }
  if (m.payload) line += `\n      payload: ${m.payload}`;
  return line;
}

const text = (s) => ({ content: [{ type: "text", text: s }] });

// ---------------------------------------------------------------------------
// server & tools

const server = new McpServer(
  { name: "intercom", version: "0.3.0" },
  {
    instructions: `Message bus between agent sessions (Claude Code, Codex, or any MCP client) running on this machine.
Start by calling join with a short descriptive name (e.g. the project you're working on).
Identities are DURABLE (v3): reuse the same name across reboots and your role/topics come back — join reattaches your existing identity instead of starting blank.
- send: fire-and-forget message to one agent or broadcast to all. Add topic to route to subscribers only. You can send to an OFFLINE identity — it queues and delivers on their next sign-in.
- ask: send a question and BLOCK until the recipient replies (or timeout). The question stays queued if they're busy.
- ask_async / wait_for_any: fire questions WITHOUT blocking (each returns its #id), then wait_for_any([ids]) for whichever answers first — fan-out coordination.
- inbox: fetch your unread messages; pass wait_seconds to long-poll, from_agent/topic to filter.
- reply: answer a question or respond to a message by id.
- who: list online sessions with live/idle/stale presence; pass include_offline to also see durable identities that are currently offline.
Delivery: on join, arm a Monitor on monitor-watcher.js for clean event-based notifications (call inbox when it fires). Messages are never typed into your terminal. Delivery is durable: anything you miss is still in your inbox. If collaborating, also check inbox between tasks.`,
  }
);

server.tool(
  "join",
  "Sign in to the intercom under a name so other sessions can find and message you. Call this first. Identities are DURABLE: re-calling from a fresh process reattaches your existing identity (role/topics restored) rather than starting blank. Re-call to rename or update subscriptions.",
  {
    name: z
      .string()
      .describe("Short kebab-case name, e.g. 'lead-engine' or 'reviewer'. Reuse the same name across reboots to keep one identity."),
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
    token: z
      .string()
      .optional()
      .describe("Identity token (from the first sign-in). Only needed to reattach an existing identity from a DIFFERENT cwd than it was created in."),
    memory_scope: z
      .string()
      .optional()
      .describe("Boot dir this identity belongs to. Defaults to cwd. Reattaching from this scope needs no token (accident-guardrail, not auth)."),
    takeover: z
      .boolean()
      .optional()
      .describe("Seize the name even if a live session currently holds it (requires the token or matching scope). Use only when reclaiming a wedged session."),
  },
  async ({ name, role, topics, token, memory_scope, takeover }) => {
    const r = signIn(name, { role, topics, token, memoryScope: memory_scope, takeover });
    const assigned = r.name;
    const others = onlineAgents().filter((a) => a.name !== assigned);
    const unread = unreadFor(assigned).length;
    let out = `joined as "${assigned}"`;
    if (r.status === "new") out += " (new identity)";
    else if (r.status === "reattach") out += " (reattached to your offline identity)";
    else if (r.status === "takeover") out += " (took over the live holder)";
    if (assigned !== sanitizeName(name)) {
      out += ` — requested "${sanitizeName(name)}" is held by another live session or a different-scope identity; you were given a distinct name. To reattach it, sign in from its scope or pass its token.`;
    }
    if (r.status === "reattach" || r.status === "takeover") {
      const rt = r.restored || {};
      const tp = rt.topics ? `, topics: ${JSON.parse(rt.topics).join(",")}` : "";
      out += `\nrestored — role: ${rt.role ?? "(none)"}${tp}`;
      out += `\n⚠ you attached to an EXISTING identity. if you are not "${assigned}", sign in under a different name.`;
      if (r.upgraded) {
        out += `\nthis identity predated persistent-identity; it's now scoped to ${process.cwd()} and issued token ${r.token} (saved 0600 to ${tokenPath(assigned)}) — needed only to reattach from another cwd.`;
      }
    }
    if (r.status === "new") {
      out += `\nidentity token: ${r.token}  (saved 0600 to ${tokenPath(assigned)}) — needed only to reattach from a cwd other than ${process.cwd()}.`;
    }
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
  "List Claude Code sessions currently online on the intercom (name, role, status, cwd, last_active time, presence: live/idle/stale). Pass active_only to hide stale agents.",
  {
    active_only: z
      .boolean()
      .optional()
      .describe("If true, only show live/idle agents (hide stale agents not seen in 5+ minutes)"),
    include_offline: z
      .boolean()
      .optional()
      .describe("Also list durable identities that are currently offline. They still receive queued messages — you can send to them and they get it on next sign-in."),
  },
  async ({ active_only, include_offline }) => {
    ensureJoined();
    let agents = onlineAgents();
    if (active_only) agents = agents.filter((a) => statusLabel(a.last_seen) !== "stale");
    const lines = agents.map((a) => {
      const presence = statusLabel(a.last_seen);
      const statusStr = a.status ? ` status:${a.status}` : "";
      const topicsStr = a.topics ? ` topics:[${JSON.parse(a.topics).join(",")}]` : "";
      return `- ${a.name}${a.name === me ? " (you)" : ""}${a.role ? ` — ${a.role}` : ""} · cwd ${a.cwd} · last_active ${ago(a.last_seen)} [${presence}]${statusStr}${topicsStr}`;
    });
    if (include_offline) {
      const onlineNames = new Set(agents.map((a) => a.name));
      const offline = allIdentities().filter((a) => !onlineNames.has(a.name) && !agentAlive(a));
      for (const a of offline) {
        const topicsStr = a.topics ? ` topics:[${JSON.parse(a.topics).join(",")}]` : "";
        lines.push(`- ${a.name}${a.role ? ` — ${a.role}` : ""} · scope ${a.memory_scope ?? a.cwd ?? "?"} · [offline · last seen ${ago(a.last_seen)}]${topicsStr}`);
      }
    }
    if (!lines.length) {
      return text(active_only ? "no active (live/idle) agents online." : "nobody online (not even you — this shouldn't happen)");
    }
    return text(lines.join("\n"));
  }
);

server.tool(
  "send",
  "Send a fire-and-forget message to another session (or broadcast to all). Add topic to route the broadcast only to subscribed agents. Add max_claims to make a broadcast claimable. Add type + payload for a structured message (typed routing — recipients can filter inbox(type:...)). Add ttl_seconds to expire it: after that it shows [STALE] and drops out of unread.",
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
    type: z
      .string()
      .optional()
      .describe("Structured message type for typed routing, e.g. 'task', 'status', 'result', 'alert'. Recipients can pull just this type with inbox(type: ...)."),
    payload: z
      .any()
      .optional()
      .describe("Structured data to carry with the message (JSON object/array, or a string). Stored verbatim and shown under the message. Pair with type."),
    ttl_seconds: z
      .number()
      .int()
      .min(1)
      .optional()
      .describe("Time-to-live: after this many seconds the message is [STALE] and no longer counts as unread. For time-sensitive coordination that shouldn't nag a recipient who missed the window."),
    max_claims: z
      .number()
      .int()
      .min(1)
      .optional()
      .describe(
        "Make this broadcast claimable: only the first max_claims agents to call claim(message_id) get the slot. " +
        "Others get 'full — already claimed by X, Y'. Only valid on broadcasts (no 'to')."
      ),
  },
  async ({ message, to, topic, type, payload, ttl_seconds, max_claims }) => {
    ensureJoined();
    if (to) {
      // A known identity is a valid recipient even when offline — the message queues
      // and is delivered on its next sign-in. Only reject a name nobody has ever used.
      if (!identityRow(to)) {
        const known = onlineAgents().map((a) => a.name).filter((n) => n !== me);
        return text(
          `no identity named "${to}". online: ${known.join(", ") || "(nobody else)"}.\n` +
          `message NOT sent — retry with a valid name (who include_offline for offline ones) or omit 'to' to broadcast.`
        );
      }
    }
    const expiresAt = ttl_seconds ? new Date(Date.now() + ttl_seconds * 1000).toISOString() : null;
    const id = insertMessage({ to, kind: "message", body: message, topic, type, payload, expiresAt });
    // Store max_claims if set (broadcasts only — silently ignore on directed sends).
    if (max_claims && !to) {
      db.prepare("UPDATE messages SET max_claims = ? WHERE id = ?").run(max_claims, id);
    }
    let out = `sent #${id} ${to ? `to ${to}` : "as broadcast"}${topic ? ` [topic:${topic}]` : ""}${type ? ` <${type}>` : ""}${ttl_seconds ? ` [ttl:${ttl_seconds}s]` : ""}${max_claims && !to ? ` [claimable:${max_claims}]` : ""}.`;
    if (to) {
      // Read-state: show recipient's last_active, or flag that it's queued for an offline identity.
      const recip = db.prepare("SELECT last_seen, pid, pid_start FROM agents WHERE name = ?").get(to);
      if (recip && agentAlive(recip)) {
        out += ` recipient last_active ${ago(recip.last_seen)} [${statusLabel(recip.last_seen)}].`;
      } else if (recip) {
        out += ` recipient is OFFLINE (last seen ${ago(recip.last_seen)}) — queued, delivered on next sign-in.`;
      }
    } else if (topic) {
      out += ` only subscribers of "${topic}" will see this.`;
    }
    if (max_claims && !to) {
      out += ` agents can claim a slot with claim(message_id: ${id}) — first ${max_claims} win.`;
    }
    out += ` they'll get it via their monitor (or next inbox check).`;
    return text(out);
  }
);

server.tool(
  "ask",
  "Ask another session a question and wait for their answer (blocks up to timeout_seconds). If they don't reply in time, the question stays queued and you can check inbox later for the answer. If the recipient is already stale (idle 5+ min, won't answer live), ask returns fast after a short grace instead of blocking the full timeout — the question is still queued and the answer still lands in your inbox.",
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
    let recipStale = false;
    if (to) {
      const recip = onlineAgents().find((a) => a.name === to);
      if (!recip) {
        const others = onlineAgents().map((a) => a.name).filter((n) => n !== me);
        return text(
          `no online agent named "${to}". online: ${others.join(", ") || "(nobody else)"}.\nquestion NOT sent.`
        );
      }
      recipStale = statusLabel(recip.last_seen) === "stale";
    }
    const qid = insertMessage({ to, kind: "question", body: question });
    // Smart stale-ask: a provably-stale recipient (no tool call in 5+ min) won't answer
    // in real time, so don't burn the full timeout blocking on them. Cap the wait to a
    // short grace window — the question is still queued and the answer still lands in
    // your inbox; we just return fast instead of stalling the caller. The grace still
    // catches an agent that's active-but-quiet (just crossed the 5-min line).
    // ponytail: fixed 6s grace; make it configurable only if a caller ever needs to.
    const STALE_GRACE_MS = 6000;
    const effectiveTimeout = recipStale ? Math.min(timeout, STALE_GRACE_MS) : timeout;
    const deadline = Date.now() + effectiveTimeout;
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
    const waited = Math.round(effectiveTimeout / 1000);
    const staleNote = recipStale
      ? ` (recipient "${to}" was stale — returned after ${waited}s instead of blocking the full ${timeout / 1000}s; they'll get it on their next inbox check.)`
      : "";
    return text(
      `no answer after ${waited}s. question #${qid} is still queued for ${to ?? "everyone"} — they'll see it on their next inbox check. check your inbox later for the answer (it will arrive as an 'answer' referencing #${qid}).${staleNote}`
    );
  }
);

server.tool(
  "ask_async",
  "Fire a question WITHOUT blocking — returns the question #id immediately. The answer lands in your inbox (as an 'answer' referencing that #id), or you can wait_for_any([id]) for it later. Use to fan out several questions in parallel, then wait_for_any for whichever answers first. Unlike ask(), this never blocks. An offline recipient still gets it on next sign-in.",
  {
    question: z.string().describe("The question to ask"),
    to: z
      .string()
      .optional()
      .describe("Agent to ask. Omit to broadcast — any agent can answer."),
  },
  async ({ question, to }) => {
    ensureJoined();
    if (to && !identityRow(to)) {
      const others = onlineAgents().map((a) => a.name).filter((n) => n !== me);
      return text(
        `no identity named "${to}". online: ${others.join(", ") || "(nobody else)"}. question NOT sent.`
      );
    }
    const qid = insertMessage({ to, kind: "question", body: question });
    return text(
      `asked #${qid}${to ? ` to ${to}` : " (broadcast)"} — non-blocking. ` +
      `the answer lands in your inbox referencing #${qid}, or call wait_for_any(question_ids: [${qid}]) to block for it.`
    );
  }
);

server.tool(
  "wait_for_any",
  "Block until the FIRST answer to any of the given question #ids arrives (or timeout). Pair with ask_async: fan out N questions, then wait_for_any for whichever replies first. Returns the answer + which question it answered. On timeout the questions stay queued and their answers still land in your inbox.",
  {
    question_ids: z
      .array(z.number().int())
      .min(1)
      .describe("The #ids returned by ask_async (or ask) to wait on."),
    timeout_seconds: z
      .number()
      .int()
      .min(1)
      .max(240)
      .optional()
      .describe("How long to wait for the first answer (default 60)."),
  },
  async ({ question_ids, timeout_seconds }) => {
    ensureJoined();
    const timeout = (timeout_seconds ?? 60) * 1000;
    const deadline = Date.now() + timeout;
    const placeholders = question_ids.map(() => "?").join(",");
    // Only answers to MY questions (addressed to me, or a broadcast answer) count — an
    // answer's to_agent is set to the original asker by reply(), so this can't return
    // an answer meant for someone else even if an unrelated #id is passed.
    const stmt = db.prepare(
      `SELECT * FROM messages WHERE kind = 'answer' AND reply_to IN (${placeholders})
         AND (to_agent = ? OR to_agent IS NULL) ORDER BY id LIMIT 1`
    );
    while (Date.now() < deadline) {
      const answer = stmt.get(...question_ids, me);
      if (answer) {
        markRead(me, [answer.id]);
        return text(`${answer.from_agent} answered question #${answer.reply_to}:\n${answer.body}`);
      }
      await sleep(400);
    }
    return text(
      `no answer to any of #${question_ids.join(", #")} after ${Math.round(timeout / 1000)}s. ` +
      `still queued — answers will land in your inbox referencing their question #id.`
    );
  }
);

server.tool(
  "reply",
  "Reply to a message or answer a question by its #id (shown in inbox). The reply is delivered to the original sender.",
  {
    message_id: z.number().int().describe("The #id of the message you're replying to"),
    message: z.string().optional().describe("Your reply / answer"),
    text: z.string().optional().describe("Alias for 'message' — your reply / answer"),
  },
  async ({ message_id, message, text: textParam }) => {
    ensureJoined();

    // Accept both 'message' and 'text' as the body param; 'text' is an alias.
    const body = message ?? textParam;
    if (!body) {
      return text(
        `reply requires either message or text param.\ncorrect signature: reply(message_id: <id>, message: "..." | text: "...")`
      );
    }

    const orig = db.prepare("SELECT * FROM messages WHERE id = ?").get(message_id);
    if (!orig) return text(`no message #${message_id} exists.`);
    if (orig.from_agent === me) return text(`message #${message_id} is your own — nothing to reply to.`);
    const kind = orig.kind === "question" ? "answer" : "message";
    const id = insertMessage({
      to: orig.from_agent,
      kind,
      body,
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
  "Fetch your unread messages (questions, answers, broadcasts). Pass wait_seconds to long-poll. Filter with from_agent, topic, or kind. Pass unread_count for a cheap count-only peek (messages stay unread). Messages are marked read once fetched (unless unread_count).",
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
    type: z
      .string()
      .optional()
      .describe("Only show structured messages of this type (see send's type param, e.g. 'task', 'result')"),
    kind: z
      .string()
      .optional()
      .describe("Only show messages of this kind: 'message', 'question', or 'answer'"),
    unread_count: z
      .boolean()
      .optional()
      .describe("If true, return just the integer count of unread messages — no bodies, no mark-as-read (cheap peek to decide whether to call inbox)"),
  },
  async ({ wait_seconds, from_agent, topic, type, kind, unread_count }) => {
    ensureJoined();
    const filters = { from: from_agent, topic, type };

    // unread_count: non-destructive peek — count only, no mark-as-read, no waiting.
    if (unread_count) {
      let msgs = unreadFor(me, filters);
      if (kind) msgs = msgs.filter((m) => m.kind === kind);
      const filterDesc = [
        from_agent && `from:${from_agent}`,
        topic && `topic:${topic}`,
        type && `type:${type}`,
        kind && `kind:${kind}`,
      ].filter(Boolean).join(", ");
      return text(`${msgs.length} unread${filterDesc ? ` (filter: ${filterDesc})` : ""}.`);
    }

    const deadline = Date.now() + (wait_seconds ?? 0) * 1000;
    let msgs = unreadFor(me, filters);
    if (kind) msgs = msgs.filter((m) => m.kind === kind);
    while (!msgs.length && Date.now() < deadline) {
      await sleep(400);
      msgs = unreadFor(me, filters);
      if (kind) msgs = msgs.filter((m) => m.kind === kind);
    }
    if (!msgs.length) {
      const filterDesc = [
        from_agent && `from:${from_agent}`,
        topic && `topic:${topic}`,
        type && `type:${type}`,
        kind && `kind:${kind}`,
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
    query: z
      .string()
      .optional()
      .describe("Full-text search query over message bodies (FTS5). Returns ranked results matching your query."),
  },
  async ({ with: withAgent, limit, query }) => {
    ensureJoined();
    let rows;

    if (query) {
      // FTS5 search: find messages matching the query, filtered to those relevant to this session.
      // Relevant = messages I sent or received or broadcasts involving me.
      // If 'with' is also specified, narrow results to that agent.
      let sql = `SELECT DISTINCT m.id FROM messages m
                 JOIN messages_fts fts ON m.id = fts.rowid
                 WHERE fts.body MATCH ?
                   AND (m.from_agent = ? OR m.to_agent = ? OR m.to_agent IS NULL)`;
      const params = [query, me, me];

      if (withAgent) {
        sql += ` AND (m.from_agent = ? OR m.to_agent = ?)`;
        params.push(withAgent, withAgent);
      }

      sql += ` ORDER BY fts.rank LIMIT ?`;
      params.push(limit ?? 30);

      const ftsMatches = db.prepare(sql).all(...params).map((r) => r.id);

      if (!ftsMatches.length) {
        return text(`no messages matching "${query}".`);
      }

      // Fetch full message rows in id order (not FTS rank order, for readability).
      const placeholders = ftsMatches.map(() => "?").join(",");
      rows = db
        .prepare(`SELECT * FROM messages WHERE id IN (${placeholders}) ORDER BY id`)
        .all(...ftsMatches);
    } else if (withAgent) {
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

    if (!rows.length) return text(query ? `no messages matching "${query}".` : "no traffic yet.");

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

server.tool(
  "thread",
  "Walk the reply-to chain both ways (ancestors and descendants) and return the full conversation in order. Shows both directions of the thread starting from the given message.",
  {
    message_id: z.number().int().describe("The #id of any message in the thread"),
  },
  async ({ message_id }) => {
    ensureJoined();

    const msg = db.prepare("SELECT * FROM messages WHERE id = ?").get(message_id);
    if (!msg) return text(`no message #${message_id} exists.`);

    // Walk up to the root (follow reply_to chain).
    let current = msg;
    while (current.reply_to) {
      current = db.prepare("SELECT * FROM messages WHERE id = ?").get(current.reply_to);
      if (!current) break;
    }
    const root = current;

    // Collect all messages in the thread by walking down from root.
    const collected = new Set();
    const allMessages = [];

    function collectAll(msg) {
      if (collected.has(msg.id)) return;
      collected.add(msg.id);
      allMessages.push(msg);

      // Find all children of this message
      const children = db
        .prepare("SELECT * FROM messages WHERE reply_to = ? ORDER BY id")
        .all(msg.id);
      for (const child of children) {
        collectAll(child);
      }
    }

    collectAll(root);

    // Sort by id to get chronological order
    allMessages.sort((a, b) => a.id - b.id);

    const lines = allMessages.map((m) => fmtMessage(m));
    return text(`thread for #${message_id} (${allMessages.length} message${allMessages.length === 1 ? "" : "s"}):\n${lines.join("\n")}`);
  }
);

server.tool(
  "digest",
  "Non-destructive catch-up summary of your unread messages: counts by kind plus one-line snippets for directed/question items. Messages stay unread — call inbox to actually read and mark them.",
  {},
  async () => {
    ensureJoined();
    const msgs = unreadFor(me);
    if (!msgs.length) return text("no unread messages.");

    // Classify each message into a display kind
    function digestKind(m) {
      if (m.kind === "question") return "question";
      if (m.kind === "answer")   return "answer";
      return m.to_agent ? "direct message" : "broadcast";
    }

    const counts = {};
    for (const m of msgs) {
      const k = digestKind(m);
      counts[k] = (counts[k] ?? 0) + 1;
    }
    const countLine = Object.entries(counts)
      .map(([k, n]) => `${n} ${k}${n !== 1 ? "s" : ""}`)
      .join(", ");

    // Notable: directed messages to me + any questions (may need a reply)
    const notable = msgs.filter((m) => m.to_agent === me || m.kind === "question");
    let out = `${msgs.length} unread: ${countLine}.`;
    if (notable.length) {
      out += `\n\ndirected/questions:`;
      for (const m of notable) {
        const snippet = m.body.length > 80 ? m.body.slice(0, 77) + "…" : m.body;
        out += `\n  [#${m.id}] from:${m.from_agent} (${digestKind(m)}): ${snippet}`;
      }
    }
    out += `\n\ncall inbox to read and mark as read.`;
    return text(out);
  }
);

server.tool(
  "get_message",
  "Fetch any message by id, regardless of read-state or direction. Useful for re-reading a broadcast or reviewing a specific message.",
  {
    message_id: z.number().int().describe("The #id of the message to fetch"),
  },
  async ({ message_id }) => {
    ensureJoined();
    const msg = db.prepare("SELECT * FROM messages WHERE id = ?").get(message_id);
    if (!msg) return text(`no message #${message_id} exists.`);
    return text(fmtMessage(msg));
  }
);

server.tool(
  "update_status",
  "Update your status or role in-place without re-joining (no name conflict, no presence reset). Visible in who() so other agents know your current state.",
  {
    status: z
      .string()
      .optional()
      .describe("Short status string, e.g. 'working', 'idle', 'done', 'blocked'"),
    role: z
      .string()
      .optional()
      .describe("Update your role text (what you're working on)"),
  },
  async ({ status, role }) => {
    if (!me) return text("not joined — call join first.");
    db.prepare(
      `UPDATE agents SET status = COALESCE(?, status), role = COALESCE(?, role), last_seen = ? WHERE name = ?`
    ).run(status ?? null, role ?? null, now(), me);
    const row = db.prepare("SELECT status, role FROM agents WHERE name = ?").get(me);
    let out = `updated ${me}:`;
    if (status !== undefined) out += ` status=${status}`;
    if (role !== undefined)   out += ` role=${role}`;
    out += `.`;
    if (row) out += ` (now: status=${row.status ?? "(none)"}, role=${row.role ?? "(none)"})`;
    return text(out);
  }
);

server.tool(
  "respond_to_broadcast",
  "Respond to a broadcast message as a worker. Stores your reply threaded to the broadcast (kind='response', reply_to=broadcast_id) so it does NOT flood the coordinator's inbox — collected on demand via collect_responses.",
  {
    message_id: z.number().int().describe("The #id of the broadcast message you are responding to"),
    body: z.string().describe("Your response body"),
  },
  async ({ message_id, body }) => {
    ensureJoined();
    const orig = db.prepare("SELECT * FROM messages WHERE id = ?").get(message_id);
    if (!orig) return text(`no message #${message_id} exists.`);
    if (orig.to_agent !== null) return text(`message #${message_id} is not a broadcast — use reply() for directed messages.`);
    const id = insertMessage({ to: null, kind: "response", body, replyTo: message_id });
    return text(`response #${id} stored (reply_to #${message_id}). coordinator collects with collect_responses(${message_id}).`);
  }
);

server.tool(
  "collect_responses",
  "Collect all worker responses to a broadcast you sent. Returns each response as 'agent: body', sorted by arrival. Does not mark them as read — call repeatedly to see new arrivals.",
  {
    message_id: z.number().int().describe("The #id of the broadcast message whose responses to collect"),
  },
  async ({ message_id }) => {
    ensureJoined();
    const orig = db.prepare("SELECT * FROM messages WHERE id = ?").get(message_id);
    if (!orig) return text(`no message #${message_id} exists.`);
    const responses = db
      .prepare("SELECT * FROM messages WHERE reply_to = ? AND kind = 'response' ORDER BY id")
      .all(message_id);
    if (!responses.length) return text(`no responses to broadcast #${message_id} yet.`);
    const lines = responses.map((r) => `${r.from_agent}: ${r.body}`);
    return text(`${responses.length} response(s) to broadcast #${message_id}:\n${lines.join("\n")}`);
  }
);

// ---------------------------------------------------------------------------
// r2-a tools: claim, assign, tasks

server.tool(
  "claim",
  "Atomically claim a slot on a claimable broadcast (one that was sent with max_claims). " +
  "First max_claims callers succeed and get 'claimed — slot k/N'. Later callers get 'full — already claimed by X, Y'. " +
  "Prevents the N-workers-race-on-the-same-task problem.",
  {
    message_id: z.number().int().describe("The #id of the claimable broadcast"),
  },
  async ({ message_id }) => {
    ensureJoined();

    // Atomic: wrap the check-and-insert in BEGIN IMMEDIATE so concurrent callers
    // can't both pass the "slots available?" check and both get a slot.
    try {
      db.exec("BEGIN IMMEDIATE");

      const msg = db.prepare("SELECT max_claims FROM messages WHERE id = ?").get(message_id);
      if (!msg) {
        db.exec("ROLLBACK");
        return text(`no message #${message_id} exists.`);
      }
      if (!msg.max_claims) {
        db.exec("ROLLBACK");
        return text(`message #${message_id} is not claimable (was not sent with max_claims). ` +
          `only the coordinator can make a broadcast claimable by setting max_claims on send.`);
      }

      const N = msg.max_claims;
      const existing = db.prepare("SELECT agent, slot FROM claims WHERE message_id = ? ORDER BY slot").all(message_id);

      // Already claimed by this agent?
      const mine = existing.find((r) => r.agent === me);
      if (mine) {
        db.exec("ROLLBACK");
        return text(`already claimed — you hold slot ${mine.slot}/${N} on #${message_id}.`);
      }

      if (existing.length >= N) {
        db.exec("ROLLBACK");
        const claimers = existing.map((r) => r.agent).join(", ");
        return text(`full — already claimed by ${claimers} (${N}/${N} slots taken). #${message_id}`);
      }

      const slot = existing.length + 1;
      db.prepare(
        "INSERT INTO claims (message_id, agent, claimed_at, slot) VALUES (?, ?, ?, ?)"
      ).run(message_id, me, now(), slot);
      db.exec("COMMIT");

      const allClaimers = [...existing.map((r) => r.agent), me].join(", ");
      return text(`claimed — slot ${slot}/${N} on #${message_id}. holders so far: ${allClaimers}.`);
    } catch (err) {
      try { db.exec("ROLLBACK"); } catch {}
      throw err;
    }
  }
);

server.tool(
  "assign",
  "Set or clear the task assignment for an agent. assign(agent, label) records who owns what. " +
  "assign(agent, '') clears that agent's assignment. See all assignments with tasks().",
  {
    agent: z.string().describe("The agent name to assign (or clear)"),
    label: z.string().describe("Short description of what they own, e.g. 'README draft'. Pass empty string to clear."),
  },
  async ({ agent, label }) => {
    ensureJoined();
    if (label === "") {
      const deleted = db.prepare("DELETE FROM assignments WHERE agent = ?").run(agent);
      return text(deleted.changes > 0 ? `cleared assignment for ${agent}.` : `${agent} had no assignment to clear.`);
    }
    db.prepare(
      "INSERT INTO assignments (agent, label, assigned_at) VALUES (?, ?, ?) ON CONFLICT(agent) DO UPDATE SET label = excluded.label, assigned_at = excluded.assigned_at"
    ).run(agent, label, now());
    return text(`assigned ${agent}: "${label}".`);
  }
);

server.tool(
  "tasks",
  "List the current who-owns-what assignment roster. Shows every agent that has an active assignment (set via assign()).",
  {},
  async () => {
    ensureJoined();
    const rows = db.prepare("SELECT agent, label, assigned_at FROM assignments ORDER BY assigned_at").all();
    if (!rows.length) return text("no assignments set. use assign(agent, label) to record who owns what.");
    const lines = rows.map((r) => `  ${r.agent}: ${r.label} (assigned ${ago(r.assigned_at)})`);
    return text(`current assignments (${rows.length}):\n${lines.join("\n")}`);
  }
);

// ---------------------------------------------------------------------------

process.on("exit", () => {
  try {
    // R4: don't delete — mark the identity offline. It stays addressable (messages
    // queue) and is reattached on next sign-in; pruneIdentities ages it out later.
    if (me) db.prepare("UPDATE agents SET left_at = ? WHERE name = ? AND pid = ?").run(now(), me, process.pid);
    db.close();
  } catch {}
});

const transport = new StdioServerTransport();
await server.connect(transport);
