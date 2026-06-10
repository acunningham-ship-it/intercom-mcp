// intercom v2 watcher tests — out-of-band delivery functionality
// Tests wait.js and monitor-watcher.js against a temp database

import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import assert from "node:assert";

const testDir = mkdtempSync(join(tmpdir(), "intercom-v2-test-"));
const DB = join(testDir, "test.db");
const SERVER = new URL("../server.js", import.meta.url).pathname;
const WAIT = new URL("../wait.js", import.meta.url).pathname;
const MONITOR = new URL("../monitor-watcher.js", import.meta.url).pathname;

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

function runWait(agentName, options = {}) {
  return new Promise((resolve, reject) => {
    const timeout =
      options.timeout !== undefined ? options.timeout : 5000;
    const args = ["--me", agentName];

    if (options.interval !== undefined)
      args.push("--interval", String(options.interval));
    if (options.monitor) args.push("--monitor");
    if (options.once) args.push("--once");

    const proc = spawn("node", [WAIT, ...args], {
      env: { ...process.env, INTERCOM_DB: DB },
    });

    let output = "";
    let timedOut = false;

    const timer = setTimeout(() => {
      timedOut = true;
      proc.kill();
      resolve(output);
    }, timeout);

    proc.stdout.on("data", (data) => {
      output += data.toString();
    });

    proc.on("exit", (code) => {
      clearTimeout(timer);
      if (!timedOut) resolve(output);
    });

    proc.on("error", reject);
  });
}

function runMonitor(agentName, options = {}) {
  return new Promise((resolve, reject) => {
    const timeout =
      options.timeout !== undefined ? options.timeout : 5000;
    const args = ["--me", agentName];

    if (options.interval !== undefined)
      args.push("--interval", String(options.interval));

    const proc = spawn("node", [MONITOR, ...args], {
      env: { ...process.env, INTERCOM_DB: DB },
    });

    let output = "";
    let timedOut = false;

    const timer = setTimeout(() => {
      timedOut = true;
      proc.kill();
      resolve(output);
    }, timeout);

    proc.stdout.on("data", (data) => {
      output += data.toString();
    });

    proc.on("exit", (code) => {
      clearTimeout(timer);
      if (!timedOut) resolve(output);
    });

    proc.on("error", reject);
  });
}

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

// Setup: create two agents
const alice = await connect("alice");
const bob = await connect("bob");

await callText(alice, "join", { name: "alice" });
await callText(bob, "join", { name: "bob" });

console.log("\n=== wait.js tests ===\n");

// Test 1: wait.js --once with no messages
console.log("Test: wait.js with no messages");
let output = await runWait("alice", { timeout: 2000, once: true });
check("no output when no messages", output.trim() === "", output);

// Test 2: wait.js detects new message
console.log("\nTest: wait.js detects new message");
const waitPromise = runWait("alice", { timeout: 3000, once: true });
await new Promise((r) => setTimeout(r, 500)); // let waiter start
await callText(bob, "send", { to: "alice", message: "hello alice" });
output = await waitPromise;
check("detects message from bob", output.includes("bob"), output);
check("includes message id", output.includes("#"), output);

// Test 3: wait.js batch mode
console.log("\nTest: wait.js batch mode");
// Send messages first, then start waiter (ensures they're already in DB before polling)
await callText(bob, "send", { to: "alice", message: "msg1" });
await callText(bob, "send", { to: "alice", message: "msg2" });
const batchPromise = runWait("alice", { timeout: 3000, once: true });
output = await batchPromise;
check(
  "batches multiple messages",
  output.includes("2 new") || output.includes("new messages"),
  output
);

// Test 4: wait.js monitor mode
console.log("\nTest: wait.js monitor mode");
const monitorPromise = runWait("alice", {
  timeout: 3000,
  once: true,
  monitor: true,
});
await new Promise((r) => setTimeout(r, 500));
await callText(bob, "send", { to: "alice", message: "montest" });
output = await monitorPromise;
check(
  "monitor mode emits structured output",
  output.includes("intercom:") && output.includes("from bob"),
  output
);

console.log("\n=== monitor-watcher.js tests ===\n");

// Test 5: monitor-watcher detects messages
console.log("Test: monitor-watcher detects messages");
const watcherPromise = runMonitor("bob", {
  timeout: 3000,
  interval: 1,
});
await new Promise((r) => setTimeout(r, 800)); // let watcher start polling
await callText(alice, "send", { to: "bob", message: "from alice" });
output = await watcherPromise;
check(
  "emits timestamp",
  /\[\d{2}:\d{2}:\d{2}\]/.test(output),
  output
);
check("includes sender", output.includes("alice"), output);
check("includes message kind", output.includes("message"), output);

// Test 6: monitor-watcher batches in same cycle
console.log("\nTest: monitor-watcher batches messages in same polling cycle");
const watcherPromise2 = runMonitor("bob", {
  timeout: 3000,
  interval: 1,
});
await new Promise((r) => setTimeout(r, 800));
await callText(alice, "send", { to: "bob", message: "m1" });
await callText(alice, "send", { to: "bob", message: "m2" });
output = await watcherPromise2;
check("reports batch count", /\d+ new/.test(output), output);

// Cleanup
await alice.close();
await bob.close();
rmSync(testDir, { recursive: true, force: true });

console.log(`\n=== results ===`);
console.log(`passed: ${passed}`);
console.log(`failed: ${failed}`);
if (failed === 0) {
  console.log("\n✓ all v2 watcher tests passed");
  process.exit(0);
} else {
  console.error(`\n✗ ${failed} test(s) failed`);
  process.exit(1);
}
