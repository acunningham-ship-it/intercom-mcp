#!/usr/bin/env node
// intercom monitor watcher — Monitor-tool-friendly notification stream
//
// Emits one line per new message, designed to be run under the Monitor tool.
// Each line is a self-contained event; the watcher can be re-armed on the next
// poll interval without side effects.
//
// Usage:
//   # Run under Claude Code's Monitor tool (all forms work)
//   node /path/to/monitor-watcher.js <agent-name> [--interval 2]
//   node /path/to/monitor-watcher.js --me <agent-name> [--interval 2]
//   node /path/to/monitor-watcher.js --server-pid <mcp-pid> [--me <fallback-name>]
//
// --server-pid binds the watcher to a SESSION rather than to a name string: the
// identity is re-resolved from the server every poll (session file, else the agents
// row for that pid), so a rename follows automatically and the watcher can never
// watch an identity its seat doesn't own. --me alone behaves exactly as before.
//
// Each line format:
//   [<timestamp>] [<kind>] <count> new from <agents>
//   e.g. [14:23:45] [message] 3 new: claude-boss (#1), worker-2 (#2-3)
//
// Emits one line per polling cycle (even if multiple messages arrive together).
// Each emission includes ALL unread messages in that cycle, grouped by sender.

import { DatabaseSync } from "node:sqlite";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join as pathJoin } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { unreadFor } from "./unread.js";

const args = process.argv.slice(2);
const opt = (flag, def) => {
  const i = args.indexOf(flag);
  return i >= 0 && args[i + 1] ? args[i + 1] : def;
};

// First positional (non-flag) arg — so the bare form `monitor-watcher.js <name>` works,
// not only `--me <name>`. CHARTER.md/builder.md document the bare form; it used to exit(2).
// Skips each `--flag` and the value that follows it (both known flags take a value).
const firstPositional = () => {
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--")) { i++; continue; }
    return args[i];
  }
  return undefined;
};

// Static fallback identity (old callers pass only this).
const staticMe = opt("--me", null) ?? firstPositional() ?? process.env.INTERCOM_NAME;
// Session binding: resolve the identity from the server owning this pid, every poll.
const serverPid = Number(opt("--server-pid", 0)) || null;
if (!staticMe && !serverPid) {
  console.error("usage: node monitor-watcher.js <name> | --me <name> | --server-pid <pid> [--interval 2]");
  process.exit(2);
}

const intervalSec = Math.max(1, Number(opt("--interval", 2)));
const intervalMs = intervalSec * 1000;

const DB_PATH =
  process.env.INTERCOM_DB ??
  pathJoin(homedir(), ".local", "share", "intercom", "intercom.db");

// Runtime dir shared with server.js (session identity files). Env-overridable so
// tests never touch the live fleet's ~/.intercom.
const RUN_DIR = process.env.INTERCOM_RUN_DIR ?? pathJoin(homedir(), ".intercom");
const SESSION_FILE = serverPid ? pathJoin(RUN_DIR, "session", String(serverPid)) : null;

let db = null;
let me = staticMe;              // current resolved identity (re-resolved every poll)
let lastSeen = new Set(); // ids we've already reported

function pidAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return err.code === "EPERM";
  }
}

// Who does the seat that owns --server-pid currently call itself? Identity lives in
// the server, not in our argv, so ask it each poll: session file first, agents row as
// backstop. Falls back to --me when there's no session binding (old callers) or the
// seat is gone.
function resolveMe() {
  if (serverPid && pidAlive(serverPid)) {
    try {
      const name = readFileSync(SESSION_FILE, "utf8").trim();
      if (name) return name;
    } catch {}
    try {
      const row = db
        ?.prepare("SELECT name FROM agents WHERE pid = ? ORDER BY last_seen DESC LIMIT 1")
        .get(serverPid);
      if (row?.name) return row.name;
    } catch {}
  }
  return staticMe;
}

function openDb() {
  try {
    db = new DatabaseSync(DB_PATH);
    db.exec("PRAGMA busy_timeout = 1000; PRAGMA journal_mode = WAL;");
    return true;
  } catch {
    return false;
  }
}

function closeDb() {
  if (db) {
    try {
      db.close();
    } catch {}
    db = null;
  }
}

if (!openDb()) {
  console.error(`Cannot open ${DB_PATH}`);
  process.exit(1);
}

function getUnread() {
  if (!me) return [];
  try {
    return unreadFor(db, me);
  } catch {
    return [];
  }
}

function formatTimestamp() {
  const d = new Date();
  return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}:${String(d.getSeconds()).padStart(2, "0")}`;
}

function groupBySender(messages) {
  const groups = {};
  for (const m of messages) {
    if (!groups[m.from_agent]) groups[m.from_agent] = [];
    groups[m.from_agent].push(m.id);
  }
  return groups;
}

function formatSenders(groups) {
  const parts = [];
  for (const sender in groups) {
    const ids = groups[sender];
    const range =
      ids.length === 1
        ? `#${ids[0]}`
        : `#${ids[0]}-${ids[ids.length - 1]}`;
    parts.push(`${sender} (${range})`);
  }
  return parts.join(", ");
}

for (const sig of ["SIGINT", "SIGTERM"]) process.on(sig, () => {
  closeDb();
  process.exit(0);
});

// Main loop: poll and emit on changes
while (true) {
  me = resolveMe();
  const messages = getUnread();
  const fresh = messages.filter((m) => !lastSeen.has(m.id));

  if (fresh.length > 0) {
    for (const m of fresh) lastSeen.add(m.id);

    const groups = groupBySender(fresh);
    const senderList = formatSenders(groups);
    const kinds = [...new Set(fresh.map((m) => m.kind))].sort().join("/");
    const ts = formatTimestamp();

    // Single-line emit: timestamp, kind, count, senders
    console.log(
      `[${ts}] [${kinds}] ${fresh.length} new: ${senderList}`
    );

    // Flush stdout so Monitor sees it immediately
    // (Node's stdout is usually line-buffered, but be explicit for safety)
    if (typeof process.stdout.flush === "function") {
      process.stdout.flush();
    }
  }

  await sleep(intervalMs);
}
