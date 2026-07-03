#!/usr/bin/env node
// intercom monitor watcher — Monitor-tool-friendly notification stream
//
// Emits one line per new message, designed to be run under the Monitor tool.
// Each line is a self-contained event; the watcher can be re-armed on the next
// poll interval without side effects.
//
// Usage:
//   # Run under Claude Code's Monitor tool (both forms work; --me wins if both given)
//   node /path/to/monitor-watcher.js <agent-name> [--interval 2]
//   node /path/to/monitor-watcher.js --me <agent-name> [--interval 2]
//
// Each line format:
//   [<timestamp>] [<kind>] <count> new from <agents>
//   e.g. [14:23:45] [message] 3 new: claude-boss (#1), worker-2 (#2-3)
//
// Emits one line per polling cycle (even if multiple messages arrive together).
// Each emission includes ALL unread messages in that cycle, grouped by sender.

import { DatabaseSync } from "node:sqlite";
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

const me = opt("--me", null) ?? firstPositional() ?? process.env.INTERCOM_NAME;
if (!me) {
  console.error("usage: node monitor-watcher.js <name> | --me <name> [--interval 2]");
  process.exit(2);
}

const intervalSec = Math.max(1, Number(opt("--interval", 2)));
const intervalMs = intervalSec * 1000;

const DB_PATH =
  process.env.INTERCOM_DB ??
  pathJoin(homedir(), ".local", "share", "intercom", "intercom.db");

let db = null;
let lastSeen = new Set(); // ids we've already reported

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
