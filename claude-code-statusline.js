#!/usr/bin/env node
// intercom-mcp statusline for Claude Code
// Shows unread message count in the status bar
//
// Usage: integrate into Claude Code's statusline hook
// Hook command: node /path/to/claude-code-statusline.js --me <agent-name>
//
// Returns JSON: { segment: "intercom", text: "📨 N", tooltip: "N unread messages" }
// or empty object if no unread messages

import { DatabaseSync } from "node:sqlite";
import { homedir } from "node:os";
import { join as pathJoin } from "node:path";
import { unreadFor } from "./unread.js";

const args = process.argv.slice(2);
const opt = (flag, def) => {
  const i = args.indexOf(flag);
  return i >= 0 && args[i + 1] ? args[i + 1] : def;
};

const me = opt("--me", process.env.INTERCOM_NAME);
if (!me) {
  // No agent name — output empty object
  console.log(JSON.stringify({}));
  process.exit(0);
}

const DB_PATH =
  process.env.INTERCOM_DB ??
  pathJoin(homedir(), ".local", "share", "intercom", "intercom.db");

try {
  const db = new DatabaseSync(DB_PATH);
  db.exec("PRAGMA busy_timeout = 1000; PRAGMA journal_mode = WAL;");

  // Count unread messages for this agent — via the shared definition, so the badge
  // can't disagree with inbox (this copy predated unread.js and already missed the
  // kind!='response', TTL and topic-routing rules; the join-time broadcast floor
  // would have widened that gap into a badge nobody can clear).
  const unreadCount = unreadFor(db, me).length;

  if (unreadCount > 0) {
    const segment = {
      segment: "intercom",
      text: `📨 ${unreadCount}`,
      tooltip: `${unreadCount} unread message${unreadCount !== 1 ? "s" : ""}`,
    };
    console.log(JSON.stringify(segment));
  } else {
    // No unread messages — output empty object (statusline hides this segment)
    console.log(JSON.stringify({}));
  }

  db.close();
} catch (err) {
  // DB error or database not ready — output empty object
  console.log(JSON.stringify({}));
  process.exit(0);
}
