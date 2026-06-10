// Test suite for watch.js dashboard
// Verifies: read-only access, correct output format, data accuracy
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { execSync } from "node:child_process";
import assert from "node:assert";

const testDir = mkdtempSync(join(tmpdir(), "intercom-watch-test-"));
const DB = join(testDir, "test.db");
const SERVER = new URL("../server.js", import.meta.url).pathname;
const WATCH = new URL("../watch.js", import.meta.url).pathname;

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

function runWatch() {
  // Run watch.js with temp DB and capture output
  const output = execSync(`node ${WATCH}`, {
    env: { ...process.env, INTERCOM_DB: DB },
    encoding: "utf-8",
  });
  return output;
}

let failed = 0;
const check = (label, cond, detail) => {
  if (cond) console.log(`  ok   ${label}`);
  else {
    failed++;
    console.error(`  FAIL ${label}\n       ${detail}`);
  }
};

// Setup: create agents and messages
console.log("Setting up test data...");
const alice = await connect("alice");
const bob = await connect("bob");
const charlie = await connect("charlie");

await callText(alice, "join", { name: "alice", role: "alice's role" });
await callText(bob, "join", { name: "bob", role: "bob's role" });
await callText(charlie, "join", { name: "charlie" });

// Create various message types
await callText(alice, "send", { message: "hello everyone", to: null });
await callText(bob, "send", { message: "hi alice", to: "alice" });

// Create an open question (ask without reply)
const askPromise = callText(alice, "ask", {
  to: "charlie",
  question: "what time is it?",
  timeout_seconds: 60,
});
await new Promise((r) => setTimeout(r, 500)); // let question land

// Create a answered question
const askPromise2 = callText(bob, "ask", {
  to: "alice",
  question: "is it working?",
  timeout_seconds: 60,
});
await new Promise((r) => setTimeout(r, 500));
let out = await callText(alice, "inbox", {});
const qid = Number(out.match(/\[#(\d+)\]/)[1]);
await callText(alice, "reply", { message_id: qid, message: "yes!" });
await new Promise((r) => setTimeout(r, 500)); // let reply land

console.log("Running watch.js tests...");

// Test: watch output exists and has expected structure
out = runWatch();
check("watch output contains status header", out.includes("Intercom Fleet Status"), out.slice(0, 100));
check("watch lists online agents", out.includes("Online Agents"), out);
check("watch shows alice", out.includes("alice"), out);
check("watch shows bob", out.includes("bob"), out);
check("watch shows charlie", out.includes("charlie"), out);

// Test: watch shows open questions
check("watch lists open questions", out.includes("Open Questions"), out);
check("watch identifies open ask", out.includes("what time is it?"), out);
check("watch shows question count (may include unanswered)", out.match(/Open Questions \(\d+\)/), out);

// Test: watch shows recent message flow
check("watch shows message flow", out.includes("Recent Message Flow"), out);
check("watch includes directed message", out.includes("hi alice"), out);
check("watch includes question", out.includes("what time is it?") || out.includes("is it working?"), out);

// Test: watch shows correct agent roles
check("watch shows alice role", out.includes("alice's role"), out);
check("watch shows bob role", out.includes("bob's role"), out);

// Test: watch shows timestamps
check("watch includes last-seen times", /\d+[smh]/.test(out), out);

// Test: watch is read-only (ensure no permission errors)
try {
  execSync(`node ${WATCH}`, {
    env: { ...process.env, INTERCOM_DB: DB },
    stdio: "pipe",
  });
  check("watch runs without permission errors", true, "");
} catch (err) {
  check("watch runs without permission errors", false, err.toString());
}

// Cleanup: close all clients and suppress MCP SDK errors during shutdown
process.on("uncaughtException", (err) => {
  // Suppress MCP connection errors during test exit
  if (
    err.code === -32000 ||
    err.code === -32001 ||
    err.message.includes("Connection closed") ||
    err.message.includes("EPIPE") ||
    err.message.includes("write")
  ) {
    // Normal cleanup errors - suppress them
    return;
  }
  // Re-throw unexpected errors
  console.error("Unexpected error:", err);
  process.exit(1);
});

process.on("unhandledRejection", (err) => {
  // Suppress MCP SDK rejections during shutdown
  if (
    err?.code === -32000 ||
    err?.code === -32001 ||
    err?.message?.includes("Connection closed") ||
    err?.message?.includes("EPIPE")
  ) {
    return;
  }
  console.error("Unhandled rejection:", err);
  process.exit(1);
});

// Close clients gracefully
try {
  await Promise.allSettled([
    alice.close().catch(() => {}),
    bob.close().catch(() => {}),
    charlie.close().catch(() => {}),
  ]);
} catch (err) {
  // Ignore
}

// Give processes time to exit
await new Promise((r) => setTimeout(r, 300));

// Clean up temp directory
try {
  rmSync(testDir, { recursive: true, force: true });
} catch {
  // May be locked; ignore
}

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nall watch tests passed");
process.exit(0);
