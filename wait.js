#!/usr/bin/env node
// intercom wait — the tmux-free wake path (v2).
//
// Long-polls the shared bus for messages addressed to --me and prints ONE line
// when new mail arrives. Pair it with your harness's background-task / monitor:
// the printed line re-invokes an otherwise-idle session, which then calls the
// intercom "inbox" tool. Needs no tmux, no pty access — the harness delivers the
// wake. Exits 0 on SIGINT/SIGTERM, on --once after the first batch, or after
// --timeout seconds.
//
//   node wait.js --me <name> [--interval 2] [--once] [--timeout 0]
//   node wait.js --me <name> --monitor       (emit one line per message, re-armable)
//
// Claude Code: run it as a background shell (one wake per mail, then re-arm), or
// under a persistent monitor (one line per new message, stays armed).

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

const me = opt("--me", process.env.INTERCOM_NAME);
if (!me) {
  console.error(
    "usage: node wait.js --me <name> [--interval 2] [--once] [--timeout 0] [--monitor]"
  );
  process.exit(2);
}
const interval = Math.max(1, Number(opt("--interval", 2))) * 1000;
const once = args.includes("--once");
const monitor = args.includes("--monitor");
const timeoutMs = Number(opt("--timeout", 0)) * 1000; // 0 = run forever

const DB_PATH =
  process.env.INTERCOM_DB ??
  pathJoin(homedir(), ".local", "share", "intercom", "intercom.db");

let db = null;

function openDb() {
  try {
    db = new DatabaseSync(DB_PATH);
    db.exec("PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL;");
    return true;
  } catch (e) {
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

const getUnread = () => {
  try {
    return unreadFor(db, me);
  } catch {
    // DB locked or tables not ready — return empty and retry next tick
    return [];
  }
};

for (const sig of ["SIGINT", "SIGTERM"]) process.on(sig, () => {
  closeDb();
  process.exit(0);
});

const announced = new Set(); // ids we've already emitted, so we don't repeat
const start = Date.now();

while (true) {
  const rows = getUnread();
  const fresh = rows.filter((r) => !announced.has(r.id));

  if (fresh.length) {
    for (const r of fresh) announced.add(r.id);

    if (monitor) {
      // Monitor mode: one line per message (re-armable)
      for (const r of fresh) {
        const target = r.to_agent ? `to ${r.to_agent}` : "broadcast";
        console.log(`intercom: #${r.id} from ${r.from_agent} ${target}`);
      }
    } else {
      // Background wake mode: batch all fresh into one line
      const parts = fresh
        .map((r) => `${r.from_agent}${r.to_agent ? "" : "→all"} #${r.id}`)
        .join(", ");
      console.log(
        `[intercom] ${fresh.length} new message${fresh.length !== 1 ? "s" : ""}: ${parts} — call the intercom "inbox" tool to read.`
      );
    }

    if (once) {
      closeDb();
      process.exit(0);
    }
  }

  if (timeoutMs && Date.now() - start >= timeoutMs) {
    closeDb();
    process.exit(0);
  }

  await sleep(interval);
}
