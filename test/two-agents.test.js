// Integration test: two (then three) separate server processes sharing one DB,
// driven over real stdio MCP transports — same topology as real Claude Code sessions.
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import assert from "node:assert";

const dir = mkdtempSync(join(tmpdir(), "intercom-test-"));
const DB = join(dir, "test.db");
const SERVER = new URL("../server.js", import.meta.url).pathname;

async function connect(label) {
  const client = new Client({ name: `test-${label}`, version: "0.0.0" });
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [SERVER],
    // Null out tmux coords so the wake path can't send-keys into the test
    // runner's own terminal; this suite exercises bus logic, not tmux waking.
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

const alice = await connect("alice");
const bob = await connect("bob");

// join
let out = await callText(alice, "join", { name: "alice", role: "testing the bus" });
check("alice joins", out.includes('joined as "alice"'), out);
out = await callText(bob, "join", { name: "bob" });
check("bob joins and sees alice", out.includes('joined as "bob"') && out.includes("alice"), out);

// who
out = await callText(alice, "who", {});
check("who lists both", out.includes("alice (you)") && out.includes("bob"), out);

// broadcast → inbox
out = await callText(alice, "send", { message: "hello everyone" });
check("broadcast sent", out.includes("broadcast"), out);
out = await callText(bob, "inbox", {});
check("bob receives broadcast", out.includes("hello everyone"), out);
out = await callText(bob, "inbox", {});
check("inbox marks read (now empty)", out.includes("inbox empty"), out);

// directed send to unknown agent
out = await callText(alice, "send", { message: "x", to: "nobody" });
check("send to unknown agent rejected", out.includes("NOT sent"), out);

// ask / reply roundtrip — alice blocks while bob answers
const askPromise = callText(alice, "ask", {
  to: "bob",
  question: "what is 2+2?",
  timeout_seconds: 20,
});
await new Promise((r) => setTimeout(r, 800)); // let the question land
out = await callText(bob, "inbox", {});
check("bob sees the question", out.includes("what is 2+2?") && out.includes("question"), out);
const qid = Number(out.match(/\[#(\d+)\]/)[1]);
out = await callText(bob, "reply", { message_id: qid, message: "4" });
check("bob replies", out.includes("replied"), out);
out = await askPromise;
check("alice's blocked ask gets the answer", out.includes("bob answered") && out.includes("4"), out);

// ask timeout path
out = await callText(alice, "ask", { to: "bob", question: "ignored q", timeout_seconds: 2 });
check("unanswered ask times out gracefully", out.includes("no answer after 2s"), out);
out = await callText(bob, "inbox", {});
check("timed-out question still queued for bob", out.includes("ignored q"), out);

// name collision
const charlie = await connect("charlie");
out = await callText(charlie, "join", { name: "alice" });
check("name collision gets suffix", out.includes('joined as "alice-2"'), out);

// history
out = await callText(alice, "history", { with: "bob" });
check("history shows the Q&A", out.includes("what is 2+2?") && out.includes("4"), out);

await alice.close();
await bob.close();
await charlie.close();
rmSync(dir, { recursive: true, force: true });

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nall checks passed");
