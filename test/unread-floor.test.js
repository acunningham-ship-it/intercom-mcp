// Test: read-state migration on rename + the join-time BROADCAST floor.
//
// Two invariants, and the second one is the dangerous one:
//   1. a renamed seat does not re-see the backlog it already read;
//   2. a NEW identity does not inherit the fleet's whole broadcast backlog —
//      BUT directed mail older than its joined_at MUST still deliver, or durable
//      offline queueing (and phantom re-pointing) silently breaks.
//
// Hermetic: temp DB + temp INTERCOM_RUN_DIR.
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { mkdtempSync, rmSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { unreadFor } from "../unread.js";

const dir = mkdtempSync(join(tmpdir(), "intercom-floor-"));
const DB = join(dir, "test.db");
const SERVER = new URL("../server.js", import.meta.url).pathname;
const env = { ...process.env, INTERCOM_DB: DB, INTERCOM_RUN_DIR: join(dir, "run"), TMUX: "", TMUX_PANE: "" };

async function connect(label) {
  const client = new Client({ name: `test-${label}`, version: "0.0.0" });
  await client.connect(new StdioClientTransport({ command: process.execPath, args: [SERVER], env }));
  return client;
}
const callText = async (client, name, args = {}) => {
  const res = await client.callTool({ name, arguments: args });
  return res.content[0].text;
};

let failed = 0;
const check = (label, cond, detail) => {
  if (cond) console.log(`  ok   ${label}`);
  else { failed++; console.error(`  FAIL ${label}\n       got: ${detail}`); }
};

// ── (a) rename carries read-state forward ───────────────────────────────────
console.log("=== (a) rename migrates read-state ===");
const boss = await connect("boss");
await callText(boss, "join", { name: "boss" });
const worker = await connect("worker");
await callText(worker, "join", { name: "worker-old" });

await callText(boss, "send", { message: "broadcast-already-read" });
let out = await callText(worker, "inbox", {});
check("worker reads the broadcast under its old name", out.includes("broadcast-already-read"), out);

await callText(worker, "join", { name: "worker-new" }); // rename mid-session
out = await callText(worker, "inbox", {});
check("renamed seat does NOT re-see what it already read",
  !out.includes("broadcast-already-read"), out);

// ── (b) the floor is broadcast-only ─────────────────────────────────────────
// Build the adversarial state directly: an identity whose joined_at is NEWER than
// both a broadcast and a directed message aimed at it. (This is exactly what a
// phantom re-point produces, and what a re-minted identity looks like.)
console.log("\n=== (b) floor hides old broadcasts, never old directed mail ===");
{
  const d = new DatabaseSync(DB);
  const old = "2020-01-01T00:00:00.000Z";
  const joined = "2020-06-01T00:00:00.000Z";
  d.prepare(
    `INSERT INTO agents (name, pid, cwd, role, joined_at, last_seen) VALUES (?, ?, ?, ?, ?, ?)`
  ).run("latecomer", 999999, "/tmp", null, joined, joined);
  const ins = d.prepare(
    `INSERT INTO messages (from_agent, to_agent, kind, body, created_at) VALUES (?, ?, 'message', ?, ?)`
  );
  ins.run("boss", null, "old-broadcast-before-join", old);
  ins.run("boss", "latecomer", "old-directed-before-join", old);
  ins.run("boss", null, "new-broadcast-after-join", "2020-12-01T00:00:00.000Z");

  const bodies = unreadFor(d, "latecomer").map((m) => m.body);
  check("broadcast older than joined_at is floored out",
    !bodies.includes("old-broadcast-before-join"), JSON.stringify(bodies));
  check("⛔ directed mail older than joined_at STILL DELIVERS",
    bodies.includes("old-directed-before-join"), JSON.stringify(bodies));
  check("broadcast after joined_at still delivers",
    bodies.includes("new-broadcast-after-join"), JSON.stringify(bodies));

  // Backward-compat: a name with no agents row (e.g. a watcher on a never-joined
  // name) must keep seeing everything rather than being floored to nothing.
  const orphan = unreadFor(d, "no-such-agent").map((m) => m.body);
  check("agent with no identity row is not floored (backward-compat)",
    orphan.includes("old-broadcast-before-join"), JSON.stringify(orphan));
  d.close();
}

// ── (c) a reattached identity still gets what it missed while offline ───────
console.log("\n=== (c) reattach keeps its original joined_at, so nothing is lost ===");
const away = await connect("away");
await callText(away, "join", { name: "away" });
await away.close();
await new Promise((r) => setTimeout(r, 300));
await callText(boss, "send", { message: "broadcast-while-away" });
await callText(boss, "send", { to: "away", message: "directed-while-away" });
const back = await connect("back");
await callText(back, "join", { name: "away" });
out = await callText(back, "inbox", {});
check("offline-queued broadcast delivers on reattach", out.includes("broadcast-while-away"), out);
check("offline-queued directed message delivers on reattach", out.includes("directed-while-away"), out);
await back.close();

await boss.close();
await worker.close();
await new Promise((r) => setTimeout(r, 200));
rmSync(dir, { recursive: true, force: true });
if (failed) { console.error(`\n${failed} check(s) failed`); process.exit(1); }
console.log("\nall unread-floor checks passed");
process.exit(0);
