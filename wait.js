#!/usr/bin/env node
// intercom wait — the tmux-free wake path.
//
// Long-polls the shared bus for messages addressed to --me and prints ONE line
// when new mail arrives. Pair it with your harness's background-task / monitor:
// the printed line re-invokes an otherwise-idle session, which then calls the
// intercom "inbox" tool. Needs no tmux, no pty access — the harness delivers the
// wake. Exits 0 on SIGINT/SIGTERM, on --once after the first batch, or after
// --timeout seconds.
//
//   node wait.js --me <name> [--interval 2] [--once] [--timeout 0]
//
// Claude Code: run it as a background shell (one wake per mail, then re-arm), or
// under a persistent monitor (one line per new message, stays armed).

import { DatabaseSync } from "node:sqlite";
import { homedir } from "node:os";
import { join as pathJoin } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

const args = process.argv.slice(2);
const opt = (flag, def) => {
  const i = args.indexOf(flag);
  return i >= 0 && args[i + 1] ? args[i + 1] : def;
};

const me = opt("--me", process.env.INTERCOM_NAME);
if (!me) {
  console.error(
    "usage: node wait.js --me <name> [--interval 2] [--once] [--timeout 0]"
  );
  process.exit(2);
}
const interval = Math.max(1, Number(opt("--interval", 2))) * 1000;
const once = args.includes("--once");
const timeoutMs = Number(opt("--timeout", 0)) * 1000; // 0 = run forever

const DB_PATH =
  process.env.INTERCOM_DB ??
  pathJoin(homedir(), ".local", "share", "intercom", "intercom.db");

const db = new DatabaseSync(DB_PATH);
db.exec("PRAGMA busy_timeout = 5000;");

// Same "unread for me" predicate the server uses for inbox.
const q = db.prepare(
  `SELECT m.id, m.from_agent, m.to_agent FROM messages m
     WHERE m.from_agent != ?
       AND (m.to_agent = ? OR m.to_agent IS NULL)
       AND NOT EXISTS (SELECT 1 FROM reads r WHERE r.agent = ? AND r.message_id = m.id)
     ORDER BY m.id`
);

for (const sig of ["SIGINT", "SIGTERM"]) process.on(sig, () => process.exit(0));

const announced = new Set(); // ids we've already emitted, so we don't repeat
const start = Date.now();

while (true) {
  let rows = [];
  try {
    rows = q.all(me, me, me);
  } catch {
    // DB/tables not ready yet (no session has joined) — retry next tick.
  }
  const fresh = rows.filter((r) => !announced.has(r.id));
  if (fresh.length) {
    for (const r of fresh) announced.add(r.id);
    const parts = fresh
      .map((r) => `${r.from_agent}${r.to_agent ? "" : "→all"} #${r.id}`)
      .join(", ");
    // One line = one wake event (batches cleanly under a monitor).
    console.log(
      `📨 intercom: ${fresh.length} new (${parts}) — call the intercom "inbox" tool to read.`
    );
    if (once) process.exit(0);
  }
  if (timeoutMs && Date.now() - start >= timeoutMs) process.exit(0);
  await sleep(interval);
}
