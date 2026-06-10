// Test: broadcast noise reduction (pull-only) and presence reclamation.
// Verifies that broadcasts don't mass-inject nudge lines, and dead sessions are reclaimed.
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import assert from "node:assert";

const dir = mkdtempSync(join(tmpdir(), "intercom-noise-test-"));
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

console.log("=== broadcast noise reduction (pull-only) ===");
const alice = await connect("alice");
const bob = await connect("bob");
const charlie = await connect("charlie");

let out = await callText(alice, "join", { name: "alice" });
check("alice joins", out.includes('joined as "alice"'), out);
out = await callText(bob, "join", { name: "bob" });
check("bob joins", out.includes('joined as "bob"'), out);
out = await callText(charlie, "join", { name: "charlie" });
check("charlie joins", out.includes('joined as "charlie"'), out);

// Directed send: would wake the recipient in tmux (fallback to pull in test env)
out = await callText(alice, "send", { message: "hello bob", to: "bob" });
check("directed send sent", out.includes("sent #") && out.includes("to bob"), out);

// Broadcast: should NOT wake anyone (pull-only)
out = await callText(alice, "send", { message: "hello everyone" });
check("broadcast reports NO nudge", !out.includes("nudged"), out);

// Verify all receivers got the broadcast (pull-only)
out = await callText(bob, "inbox", {});
check("bob pulls the broadcast", out.includes("hello everyone"), out);
out = await callText(charlie, "inbox", {});
check("charlie pulls the broadcast", out.includes("hello everyone"), out);

// Directed question: should wake the recipient
out = await callText(alice, "ask", { to: "bob", question: "what's your name?", timeout_seconds: 2 });
check("directed ask times out", out.includes("no answer after 2s"), out);

// Broadcast question: should NOT wake anyone (pull-only)
out = await callText(alice, "ask", { question: "who's online?", timeout_seconds: 2 });
check("broadcast ask times out", out.includes("no answer after 2s"), out);

// Verify receiver pulls the broadcast question
out = await callText(bob, "inbox", {});
check("bob pulls the broadcast question", out.includes("who's online?"), out);

console.log("\n=== presence reclaim (dead session recovery) ===");

await alice.close();
await bob.close();
await charlie.close();

// Simulate a new session reclaiming alice's name
const alice2 = await connect("alice2");
out = await callText(alice2, "join", { name: "alice" });
check(
  "new process reclaims dead alice name",
  out.includes('joined as "alice"') && !out.includes("taken"),
  out
);

// Verify alice2 can receive messages
const bob2 = await connect("bob2");
await callText(bob2, "join", { name: "bob" });
out = await callText(bob2, "send", { message: "hello alice", to: "alice" });
check("bob can send to reclaimed name", out.includes("sent"), out);
out = await callText(alice2, "inbox", {});
check("alice2 receives message", out.includes("hello alice"), out);

await alice2.close();
await bob2.close();
rmSync(dir, { recursive: true, force: true });

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nall checks passed");
