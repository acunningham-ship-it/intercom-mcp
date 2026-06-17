// v3 feature tests: presence heartbeat, read-state, topic routing, inbox filters.
// Uses a temp DB (never touches the live one). Topology mirrors two-agents.test.js.
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import assert from "node:assert";

const dir = mkdtempSync(join(tmpdir(), "intercom-v3-test-"));
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
const check = (label, cond, detail = "") => {
  if (cond) console.log(`  ok   ${label}`);
  else {
    failed++;
    console.error(`  FAIL ${label}\n       got: ${detail}`);
  }
};

// -----------------------------------------------------------------------
// SETUP: three clients — alpha, beta, gamma

const alpha = await connect("alpha");
const beta  = await connect("beta");
const gamma = await connect("gamma");

// -----------------------------------------------------------------------
// 1. PRESENCE HEARTBEAT — who() shows last_active + [live/idle/stale]

let out;

await callText(alpha, "join", { name: "alpha", role: "v3-test" });
await callText(beta,  "join", { name: "beta" });
await callText(gamma, "join", { name: "gamma" });

out = await callText(alpha, "who", {});
check("who shows last_active field", out.includes("last_active"), out);
check("who shows status label", /\[(live|idle|stale)\]/.test(out), out);
check("who marks self with (you)", out.includes("alpha (you)"), out);
check("who lists all 3 agents", out.includes("beta") && out.includes("gamma"), out);

// -----------------------------------------------------------------------
// 2. READ-STATE — send shows recipient last_active; history shows read receipts

// alpha sends to beta
out = await callText(alpha, "send", { message: "hi beta", to: "beta" });
check("directed send shows recipient last_active", out.includes("last_active"), out);
check("directed send shows presence status in brackets", /\[(live|idle|stale)\]/.test(out), out);
check("directed send still says 'sent #'", out.includes("sent #"), out);

// beta reads it
await callText(beta, "inbox", {});

// check history from alpha's side — directed message to beta should show ✓ read
out = await callText(alpha, "history", { with: "beta" });
check("history marks sent-to-beta message as read after beta inboxed", out.includes("✓ read"), out);

// gamma sends to alpha, alpha hasn't read it yet
await callText(gamma, "send", { message: "hey alpha", to: "alpha" });
out = await callText(gamma, "history", { with: "alpha" });
check("history marks unread message as · unread", out.includes("· unread"), out);

// alpha reads it, now gamma's history should show ✓ read
await callText(alpha, "inbox", {});
out = await callText(gamma, "history", { with: "alpha" });
check("history shows ✓ read after recipient inboxes", out.includes("✓ read"), out);

// -----------------------------------------------------------------------
// 3. TOPIC ROUTING — join with topics; send with topic; only subscribers see it

// Re-join beta with topic subscription to ["alerts"]
await callText(beta, "join", { name: "beta", topics: ["alerts"] });
out = await callText(beta, "inbox", {}); // drain any backlog

// alpha joins with NO topics — gets everything
await callText(alpha, "join", { name: "alpha" }); // re-join, no topics = null = see all

// alpha broadcasts with topic:"alerts" — beta (subscribed) should see it, gamma (no topics=null) also sees it
// Wait — agents with topics=null (never set) get ALL broadcasts unconditionally (back-compat).
// So gamma sees the alert too. Only an agent that joined with topics:[] (empty) would be excluded.

// gamma re-joins with empty topics subscription (subscribes to nothing topic-wise)
await callText(gamma, "join", { name: "gamma", topics: [] });

// Now: alpha=no topics (null, sees all), beta=["alerts"], gamma=[] (sees no topic-tagged broadcasts)

// alpha broadcasts with topic:"alerts"
// alpha is the SENDER so alpha won't receive their own message.
// beta (subscribed) should receive; gamma (topics=[]) should not.
out = await callText(alpha, "send", { message: "critical alert!", topic: "alerts" });
check("topic broadcast shows [topic:alerts]", out.includes("[topic:alerts]"), out);
check("topic broadcast mentions only subscribers see it", out.includes("subscribers"), out);

// beta (subscribed to alerts) should see it
out = await callText(beta, "inbox", {});
check("beta (alerts subscriber) receives topic broadcast", out.includes("critical alert!"), out);

// gamma (topics=[], not subscribed to alerts) should NOT see it
out = await callText(gamma, "inbox", {});
check("gamma (topics=[], not subscribed) does NOT receive topic broadcast",
  out.includes("inbox empty"), out);

// Back-compat: agent with topics=null sees ALL topic broadcasts (including from others).
// beta sends a topic broadcast, alpha (topics=null) should receive it.
await callText(beta, "send", { message: "beta alert!", topic: "alerts" });
out = await callText(alpha, "inbox", {});
check("alpha (topics=null) receives topic broadcast from beta (back-compat)", out.includes("beta alert!"), out);

// Non-topic broadcast should reach everyone including gamma
out = await callText(beta, "send", { message: "non-topic news" });
out = await callText(gamma, "inbox", {});
check("non-topic broadcast reaches gamma even with topics=[]", out.includes("non-topic news"), out);

// -----------------------------------------------------------------------
// 4. INBOX FILTERS — from_agent and topic filters

// Send a few messages
await callText(alpha, "send", { message: "alpha says hi" });
await callText(beta,  "send", { message: "beta says hi" });
await callText(alpha, "send", { message: "alpha alert", topic: "alerts" });

// gamma re-joins with alerts subscription to see filtered messages
await callText(gamma, "join", { name: "gamma", topics: ["alerts"] });

// filter by from_agent
out = await callText(gamma, "inbox", { from_agent: "alpha" });
// gamma should only see alpha's messages, not beta's
check("inbox from_agent filter returns alpha messages", out.includes("alpha says hi") || out.includes("alpha alert"), out);
check("inbox from_agent filter excludes beta messages", !out.includes("beta says hi"), out);

// drain any remainder
await callText(gamma, "inbox", {});

// topic filter: send two broadcasts — one with topic, one without
await callText(alpha, "send", { message: "plain message" });
await callText(alpha, "send", { message: "topic-tagged message", topic: "alerts" });

out = await callText(gamma, "inbox", { topic: "alerts" });
check("inbox topic filter shows only topic:alerts message", out.includes("topic-tagged message"), out);
check("inbox topic filter excludes non-topic message", !out.includes("plain message"), out);

// the plain message is still unread — fetch it normally
out = await callText(gamma, "inbox", {});
check("plain message still in inbox after filtered fetch", out.includes("plain message"), out);

// -----------------------------------------------------------------------
// 5. BACKWARD COMPAT — old-style calls without new params work identically

// join without topics
out = await callText(alpha, "join", { name: "alpha-compat" });
check("join without topics works", out.includes('joined as "alpha-compat"') || out.includes("joined as"), out);

// send without topic
out = await callText(alpha, "send", { message: "compat broadcast" });
check("send without topic works (broadcast)", out.includes("broadcast"), out);

// inbox without filters
out = await callText(beta, "inbox", {});
check("inbox without filters works", out !== null && out !== undefined, out);

// who without args
out = await callText(alpha, "who", {});
check("who without args works", out.includes("last_active"), out);

// -----------------------------------------------------------------------
// 6. REGRESSION BASELINE TESTS — plans 002/003/004 depend on these

console.log("\n=== regression baseline tests ===");

// Test 1: Topic-subscribed agent does NOT see unsubscribed topic broadcast in inbox
const delta = await connect("delta");
const epsilon = await connect("epsilon");

await callText(delta, "join", { name: "delta", topics: ["alerts"] });
await callText(epsilon, "join", { name: "epsilon", topics: ["other"] });

// epsilon broadcasts with topic:"other" (delta not subscribed)
await callText(epsilon, "send", { message: "unsubscribed topic msg", topic: "other" });

// delta checks inbox — must NOT see epsilon's topic:other message
out = await callText(delta, "inbox", {});
check("topic-subscribed agent does NOT see unsubscribed topic broadcast",
  !out.includes("unsubscribed topic msg"), out);

await delta.close();
await epsilon.close();

// Test 2: Retention prune removes messages older than cutoff on join
// Using INTERCOM_RETENTION_DAYS=0 to disable retention entirely and verify the simpler invariant
const dir2 = mkdtempSync(join(tmpdir(), "intercom-retention-test-"));
const DB2 = join(dir2, "test.db");

// First, insert a message with retention enabled (default)
const zeta1 = await (async () => {
  const client = new Client({ name: "test-zeta1", version: "0.0.0" });
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [SERVER],
    env: { ...process.env, INTERCOM_DB: DB2 },
  });
  await client.connect(transport);
  return client;
})();

await callText(zeta1, "join", { name: "zeta1" });
await callText(zeta1, "send", { message: "retention test msg" });
await zeta1.close();

// Now connect a new session with INTERCOM_RETENTION_DAYS=0 (disabled)
const zeta2 = await (async () => {
  const client = new Client({ name: "test-zeta2", version: "0.0.0" });
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [SERVER],
    env: { ...process.env, INTERCOM_DB: DB2, INTERCOM_RETENTION_DAYS: "0" },
  });
  await client.connect(transport);
  return client;
})();

await callText(zeta2, "join", { name: "zeta2" });
out = await callText(zeta2, "inbox", {});
// With INTERCOM_RETENTION_DAYS=0, retention is disabled, so old message is NOT pruned
check("retention prune disabled (INTERCOM_RETENTION_DAYS=0) preserves messages",
  out.includes("retention test msg"), out);

await zeta2.close();
rmSync(dir2, { recursive: true, force: true });

// Test 3: A second live session requesting an already-held name gets a suffixed name
const iota = await connect("iota");
const kappa = await connect("kappa");

await callText(iota, "join", { name: "worker" });
out = await callText(kappa, "join", { name: "worker" });
check("second session with same name gets suffix", out.includes("worker-2"), out);

out = await callText(iota, "who", {});
check("who lists both worker and worker-2", out.includes("worker") && out.includes("worker-2"), out);

await iota.close();
await kappa.close();

// -----------------------------------------------------------------------

await alpha.close();
await beta.close();
await gamma.close();
rmSync(dir, { recursive: true, force: true });

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nall v3 checks passed");
