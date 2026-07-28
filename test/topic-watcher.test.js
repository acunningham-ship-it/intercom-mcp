// Regression test: unreadFor() (the shared watcher/server function) applies
// topic routing correctly. An agent subscribed to topics:["alerts"] must NOT
// be woken for a broadcast tagged topic:"other", but MUST see topic:"alerts"
// broadcasts, non-topic broadcasts, and directed messages.
//
// Tests the shared unread.js module directly against a seeded in-memory DB
// (no MCP server, no watcher process — isolates the routing logic itself).

import { DatabaseSync } from "node:sqlite";
import assert from "node:assert";
import { unreadFor } from "../unread.js";

let passed = 0;
let failed = 0;

const check = (label, cond, detail = "") => {
  if (cond) {
    console.log(`  ✓ ${label}`);
    passed++;
  } else {
    console.error(`  ✗ ${label}`);
    if (detail) console.error(`    ${detail}`);
    failed++;
  }
};

// ---- setup: in-memory DB with schema matching server.js ----
const db = new DatabaseSync(":memory:");
db.exec(`
  PRAGMA journal_mode = WAL;
  CREATE TABLE IF NOT EXISTS agents (
    name        TEXT PRIMARY KEY,
    pid         INTEGER NOT NULL,
    cwd         TEXT,
    role        TEXT,
    joined_at   TEXT NOT NULL,
    last_seen   TEXT NOT NULL,
    tmux_socket TEXT,
    tmux_pane   TEXT,
    topics      TEXT
  );
  CREATE TABLE IF NOT EXISTS messages (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    from_agent TEXT NOT NULL,
    to_agent   TEXT,
    kind       TEXT NOT NULL DEFAULT 'message',
    body       TEXT NOT NULL,
    reply_to   INTEGER,
    created_at TEXT NOT NULL,
    topic      TEXT,
    type       TEXT,
    payload    TEXT,
    expires_at TEXT
  );
  CREATE TABLE IF NOT EXISTS reads (
    agent      TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    PRIMARY KEY (agent, message_id)
  );
`);

const now = new Date().toISOString();

// subscriber: subscribed to ["alerts"] only
db.prepare(
  "INSERT INTO agents (name, pid, joined_at, last_seen, topics) VALUES (?, ?, ?, ?, ?)"
).run("subscriber", 1, now, now, JSON.stringify(["alerts"]));

// sender: topics=NULL (no subscription, sends messages)
db.prepare(
  "INSERT INTO agents (name, pid, joined_at, last_seen, topics) VALUES (?, ?, ?, ?, ?)"
).run("sender", 2, now, now, null);

// unsubscribed-topic broadcast (should be EXCLUDED for subscriber)
const { lastInsertRowid: otherId } = db.prepare(
  "INSERT INTO messages (from_agent, to_agent, kind, body, created_at, topic) VALUES (?, ?, ?, ?, ?, ?)"
).run("sender", null, "message", "broadcast to other topic", now, "other");

// subscribed-topic broadcast (should be INCLUDED)
const { lastInsertRowid: alertsId } = db.prepare(
  "INSERT INTO messages (from_agent, to_agent, kind, body, created_at, topic) VALUES (?, ?, ?, ?, ?, ?)"
).run("sender", null, "message", "broadcast to alerts topic", now, "alerts");

// non-topic broadcast (should be INCLUDED — no topic set)
const { lastInsertRowid: noTopicId } = db.prepare(
  "INSERT INTO messages (from_agent, to_agent, kind, body, created_at, topic) VALUES (?, ?, ?, ?, ?, ?)"
).run("sender", null, "message", "plain broadcast", now, null);

// directed message to subscriber (should be INCLUDED — bypasses routing)
const { lastInsertRowid: directedId } = db.prepare(
  "INSERT INTO messages (from_agent, to_agent, kind, body, created_at, topic) VALUES (?, ?, ?, ?, ?, ?)"
).run("sender", "subscriber", "message", "direct to subscriber", now, null);

// ---- run unreadFor ----
const msgs = unreadFor(db, "subscriber");
const ids = msgs.map((m) => Number(m.id));

console.log("\n=== topic-watcher regression tests ===\n");

check(
  "unsubscribed topic broadcast (topic:other) is EXCLUDED",
  !ids.includes(Number(otherId)),
  `ids seen: ${JSON.stringify(ids)}, expected ${otherId} absent`
);

check(
  "subscribed topic broadcast (topic:alerts) is INCLUDED",
  ids.includes(Number(alertsId)),
  `ids seen: ${JSON.stringify(ids)}, expected ${alertsId} present`
);

check(
  "non-topic broadcast is INCLUDED",
  ids.includes(Number(noTopicId)),
  `ids seen: ${JSON.stringify(ids)}, expected ${noTopicId} present`
);

check(
  "directed message bypasses topic routing and is INCLUDED",
  ids.includes(Number(directedId)),
  `ids seen: ${JSON.stringify(ids)}, expected ${directedId} present`
);

check(
  "exactly 3 messages returned (alerts + no-topic + directed, not other)",
  ids.length === 3,
  `got ${ids.length} messages: ${JSON.stringify(ids)}`
);

// Also verify topics=NULL agent sees everything (backward-compat path)
const msgsNull = unreadFor(db, "sender");
// sender sent all messages so sees none (from_agent != sender filters them out)
// but let's seed a separate null-topics agent to verify the backward-compat path

const { lastInsertRowid: broadcastOther2 } = db.prepare(
  "INSERT INTO messages (from_agent, to_agent, kind, body, created_at, topic) VALUES (?, ?, ?, ?, ?, ?)"
).run("subscriber", null, "message", "reverse broadcast other", now, "other");

const { lastInsertRowid: broadcastAlerts2 } = db.prepare(
  "INSERT INTO messages (from_agent, to_agent, kind, body, created_at, topic) VALUES (?, ?, ?, ?, ?, ?)"
).run("subscriber", null, "message", "reverse broadcast alerts", now, "alerts");

const msgsNullAgent = unreadFor(db, "sender");
const nullIds = msgsNullAgent.map((m) => Number(m.id));

check(
  "topics=NULL agent sees topic:other broadcast (backward-compat)",
  nullIds.includes(Number(broadcastOther2)),
  `null-topics agent ids: ${JSON.stringify(nullIds)}`
);

check(
  "topics=NULL agent sees topic:alerts broadcast (backward-compat)",
  nullIds.includes(Number(broadcastAlerts2)),
  `null-topics agent ids: ${JSON.stringify(nullIds)}`
);

db.close();

console.log(`\n=== results ===`);
console.log(`passed: ${passed}`);
console.log(`failed: ${failed}`);
if (failed === 0) {
  console.log("\n✓ all topic-watcher tests passed");
  process.exit(0);
} else {
  console.error(`\n✗ ${failed} test(s) failed`);
  process.exit(1);
}
