#!/usr/bin/env node
// intercom watch — live, read-only dashboard of the agent fleet bus
// Shows: who's online, recent message flow, who's waiting on whom (open asks)
// Usage: node watch.js [--follow]
//   (--follow for continuous updates; default is one snapshot)

import { DatabaseSync } from "node:sqlite";
import { homedir } from "node:os";
import { join as pathJoin } from "node:path";

const DB_PATH =
  process.env.INTERCOM_DB ??
  pathJoin(homedir(), ".local", "share", "intercom", "intercom.db");

const FOLLOW = process.argv.includes("--follow");
const REFRESH_MS = 2000; // poll every 2 seconds in follow mode

let db = null;

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
  if (s < 60) return `${Math.round(s)}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  if (s < 86400) return `${Math.round(s / 3600)}h`;
  return `${Math.round(s / 86400)}d`;
}

function openDb() {
  try {
    db = new DatabaseSync(DB_PATH, { open: true });
    db.exec("PRAGMA query_only = 1"); // enforce read-only
    return true;
  } catch (err) {
    console.error(`Cannot open ${DB_PATH}: ${err.message}`);
    return false;
  }
}

function closeDb() {
  if (db) {
    db.close();
    db = null;
  }
}

function getOnlineAgents() {
  const rows = db.prepare("SELECT * FROM agents ORDER BY joined_at").all();
  const online = [];
  for (const a of rows) {
    if (pidAlive(a.pid)) {
      online.push(a);
    }
  }
  return online;
}

function getOpenAsks() {
  // Questions without a corresponding answer
  const questions = db
    .prepare(`
      SELECT m.id, m.from_agent, m.to_agent, m.body, m.created_at,
             COUNT(a.id) as replied_count
      FROM messages m
      LEFT JOIN messages a ON a.reply_to = m.id AND a.kind = 'answer'
      WHERE m.kind = 'question'
      GROUP BY m.id
      HAVING replied_count = 0
      ORDER BY m.created_at DESC
    `)
    .all();
  return questions;
}

function getRecentMessages(limit = 20) {
  return db
    .prepare(`
      SELECT * FROM messages
      ORDER BY id DESC
      LIMIT ?
    `)
    .all(limit)
    .reverse(); // show chronological order (oldest first)
}

function formatOnlineAgents(agents) {
  if (agents.length === 0) {
    return "  (no agents online)";
  }
  const lines = agents.map((a) => {
    const role = a.role ? ` · ${a.role}` : "";
    const cwd = a.cwd ? ` @ ${a.cwd}` : "";
    return `  ${a.name} (pid ${a.pid})${role}${cwd} — ${ago(a.last_seen)} ago`;
  });
  return lines.join("\n");
}

function formatOpenAsks(asks) {
  if (asks.length === 0) {
    return "  (no open questions)";
  }
  const lines = asks.map((q) => {
    const target = q.to_agent ? `to ${q.to_agent}` : "broadcast";
    const body = q.body.split("\n")[0].slice(0, 60); // first line, truncated
    return `  #${q.id}: ${q.from_agent} ${target} (${ago(q.created_at)}): "${body}"`;
  });
  return lines.join("\n");
}

function formatRecentMessages(messages) {
  if (messages.length === 0) {
    return "  (no messages)";
  }
  const lines = messages.map((m) => {
    const target = m.to_agent ? `→ ${m.to_agent}` : "→ broadcast";
    const kind = m.kind === "message" ? "" : ` [${m.kind}]`;
    const body = m.body.split("\n")[0].slice(0, 60);
    return `  #${m.id} ${m.from_agent} ${target}${kind} (${ago(m.created_at)}): "${body}"`;
  });
  return lines.join("\n");
}

function printSnapshot() {
  const agents = getOnlineAgents();
  const asks = getOpenAsks();
  const messages = getRecentMessages(15);

  // Clear screen if in follow mode
  if (FOLLOW) process.stdout.write("[2J[H");

  console.log("┌─ Intercom Fleet Status " + new Date().toISOString().slice(11, 19));
  console.log("│");
  console.log("├─ Online Agents (" + agents.length + ")");
  console.log(formatOnlineAgents(agents));
  console.log("│");
  console.log("├─ Open Questions (" + asks.length + ")");
  console.log(formatOpenAsks(asks));
  console.log("│");
  console.log("├─ Recent Message Flow (last 15)");
  console.log(formatRecentMessages(messages));
  console.log("│");
  console.log("└─ (press ^C to exit)");
}

async function main() {
  if (!openDb()) process.exit(1);

  try {
    if (FOLLOW) {
      // Continuous update mode
      while (true) {
        try {
          printSnapshot();
          await new Promise((r) => setTimeout(r, REFRESH_MS));
        } catch (err) {
          // Reopen db in case it was locked
          closeDb();
          if (!openDb()) {
            console.error("Lost database connection");
            process.exit(1);
          }
        }
      }
    } else {
      // Single snapshot
      printSnapshot();
    }
  } finally {
    closeDb();
  }
}

main().catch(console.error);
