// Test: PID reuse hardening (plan 004).
// Verifies that a row with a live PID but mismatched pid_start is treated as dead/pruned,
// and that rows with pid_start = NULL are treated as alive (backward-compat).
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { mkdtempSync, rmSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import assert from "node:assert";

const dir = mkdtempSync(join(tmpdir(), "intercom-pid-reuse-"));
const DB = join(dir, "test.db");
const SERVER = new URL("../server.js", import.meta.url).pathname;

async function connect(label) {
  const client = new Client({ name: `test-${label}`, version: "0.0.0" });
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [SERVER],
    env: { ...process.env, INTERCOM_DB: DB, TMUX: "", TMUX_PANE: "" },
  });
  await client.connect(transport);
  return client;
}

const callText = async (client, name, args = {}) => {
  const res = await client.callTool({ name, arguments: args });
  return res.content[0].text;
};

let failed = 0;
const check = (label, cond, detail) => {
  if (cond) console.log(`  ok   ${label}`);
  else {
    failed++;
    console.error(`  FAIL ${label}\n       got: ${detail}`);
  }
};

// ── Part 1: mismatched pid_start → pruned ────────────────────────────────────
console.log("=== PID reuse: mismatched pid_start → pruned ===");

const s1 = await connect("s1");
let out = await callText(s1, "join", { name: "alice" });
check("alice joins", out.includes('joined as "alice"'), out);

out = await callText(s1, "who", {});
check("alice visible in who before corruption", out.includes("alice"), out);

// Corrupt alice's pid_start to simulate a reused PID (dead agent's row)
{
  const db = new DatabaseSync(DB);
  db.prepare("UPDATE agents SET pid_start = ? WHERE name = ?").run("999999999", "alice");
  db.close();
}

// who triggers onlineAgents → agentAlive detects mismatch → prunes alice's row
out = await callText(s1, "who", {});
check("alice pruned from who on pid_start mismatch", !out.includes("alice"), out);

// Confirm the row is actually gone from the DB
{
  const db = new DatabaseSync(DB);
  const row = db.prepare("SELECT * FROM agents WHERE name = ?").get("alice");
  check("alice row deleted from DB", row === undefined, JSON.stringify(row));
  db.close();
}

// ── Part 2: NULL pid_start → treated as alive (backward-compat) ──────────────
console.log("\n=== backward-compat: NULL pid_start + alive PID → online ===");

// Insert a legacy row (pre-004) using our own PID (guaranteed alive) and NULL pid_start
{
  const ts = new Date().toISOString();
  const db = new DatabaseSync(DB);
  db.prepare(
    `INSERT OR REPLACE INTO agents (name, pid, pid_start, cwd, role, joined_at, last_seen)
     VALUES (?, ?, NULL, ?, ?, ?, ?)`
  ).run("legacy-agent", process.pid, process.cwd(), "pre-004 session", ts, ts);
  db.close();
}

out = await callText(s1, "who", {});
check("legacy null pid_start agent stays online", out.includes("legacy-agent"), out);

// ── Part 3: correct pid_start → online ───────────────────────────────────────
console.log("\n=== correct pid_start → agent stays online ===");

// Join a fresh session — it will store the real pid_start
const s2 = await connect("s2");
out = await callText(s2, "join", { name: "bob" });
check("bob joins with real pid_start", out.includes('joined as "bob"'), out);

out = await callText(s1, "who", {});
check("bob visible in who with correct pid_start", out.includes("bob"), out);

await s1.close();
await s2.close();
rmSync(dir, { recursive: true, force: true });

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nall checks passed");
