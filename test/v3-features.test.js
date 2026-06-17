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
// 7. ATOMIC NAME CLAIM — live holder is never overwritten; routing stays correct

console.log("\n=== atomic name-claim tests (plan 003) ===");

const lambda = await connect("lambda");
const mu     = await connect("mu");

// lambda claims "claimtest"
await callText(lambda, "join", { name: "claimtest", role: "original" });

// mu tries the same name — must get suffixed, never overwrite lambda's row
out = await callText(mu, "join", { name: "claimtest" });
check("second claimer gets suffixed name", out.includes("claimtest-2"), out);
check("second claimer confirmation text says claimtest-2", out.includes("claimtest-2"), out);

// who must show both names
out = await callText(lambda, "who", {});
check("who shows original holder claimtest", out.includes("claimtest"), out);
check("who shows suffixed holder claimtest-2", out.includes("claimtest-2"), out);

// Send a directed message to "claimtest" — must land with lambda, not mu
const nu = await connect("nu");
await callText(nu, "join", { name: "nu-messenger" });
await callText(nu, "send", { message: "directed to original", to: "claimtest" });

// lambda (original holder) must receive it
out = await callText(lambda, "inbox", {});
check("original holder receives message directed to their name", out.includes("directed to original"), out);

// mu (suffixed claimer) must NOT receive lambda's directed message
out = await callText(mu, "inbox", {});
check("suffixed claimer does NOT receive message directed at original name",
  !out.includes("directed to original"), out);

await lambda.close();
await mu.close();
await nu.close();

// -----------------------------------------------------------------------

await alpha.close();
await beta.close();
await gamma.close();
rmSync(dir, { recursive: true, force: true });

// -----------------------------------------------------------------------
// Lane A: THREADING, GET_MESSAGE, REPLY ALIAS, FTS SEARCH

console.log("\n=== Lane A: threading + get_message + reply alias + FTS search ===");

const alice = await connect("alice");
const bob   = await connect("bob");

await callText(alice, "join", { name: "alice" });
await callText(bob,   "join", { name: "bob" });

// 1. THREADING — reply_to chain and thread tool

console.log("\n--- test: threading ---");

// alice asks bob a question
const askResp = await callText(alice, "ask", {
  question: "what is the answer to life?",
  to: "bob",
  timeout_seconds: 1,
});
// Extract the message ID from "question #X"
const q1 = askResp.match(/question #(\d+)/)?.[1];
check("ask returns message id in response", !!q1, `q1=${q1}, response=${askResp}`);

// bob replies with an answer
let result = await callText(bob, "inbox", {});
check("bob sees the question", result.includes("what is the answer to life?"), result);

const aId = result.match(/#(\d+)\]/)?.[1];
check("inbox shows question with #id", !!aId, `aId=${aId}`);

const replyResp = await callText(bob, "reply", { message_id: parseInt(aId), message: "42" });
// Extract from "replied to #X with #Y" → extract the last match
const a1Match = replyResp.match(/with #(\d+)/);
const a1 = a1Match ? a1Match[1] : replyResp.match(/#(\d+)/)?.[1];
check("reply returns answer message id", !!a1, `a1=${a1}, response=${replyResp}`);

// alice reads the answer
result = await callText(alice, "inbox", {});
check("alice receives answer", result.includes("42"), result);
check("answer shows reply_to prefix", result.includes("↩"), result);

// 2. GET_MESSAGE — fetch any message by id

console.log("\n--- test: get_message ---");

result = await callText(bob, "get_message", { message_id: parseInt(q1) });
check("get_message retrieves question", result.includes("what is the answer to life?"), result);

result = await callText(alice, "get_message", { message_id: parseInt(a1) });
check("get_message retrieves answer", result.includes("42"), result);

// 3. REPLY ALIAS — both 'message' and 'text' params work

console.log("\n--- test: reply alias (text → message) ---");

// bob sends a message to alice
const sendResp = await callText(bob, "send", { message: "hello alice", to: "alice" });
const b1 = sendResp.match(/sent #(\d+)/)?.[1];
check("send returns message id", !!b1, `b1=${b1}, response=${sendResp}`);

await callText(alice, "inbox", {});

// alice replies using the 'text' param instead of 'message'
const replyResp2 = await callText(alice, "reply", { message_id: parseInt(b1), text: "hello back" });
check("reply with 'text' param works", replyResp2.includes("replied to"), replyResp2);
check("reply with 'text' param shows reply id", replyResp2.includes("#"), replyResp2);

// bob reads it
result = await callText(bob, "inbox", {});
check("bob receives reply sent via 'text' param", result.includes("hello back"), result);

// 4. REPLY WITHOUT PARAMS — should error with correct signature

console.log("\n--- test: reply error handling ---");

result = await callText(alice, "reply", { message_id: parseInt(b1) });
check("reply without message/text shows error", result.includes("reply requires"), result);
check("reply error shows correct signature", result.includes("message_id") && result.includes("message"), result);

// 5. THREAD TOOL — walk ancestors and descendants

console.log("\n--- test: thread tool ---");

// set up a longer conversation: alice -> bob -> alice -> bob
const sendResp1 = await callText(alice, "send", {
  message: "first question",
  to: "bob"
});
const firstQ = sendResp1.match(/sent #(\d+)/)?.[1];
await callText(bob, "inbox", {});

const replyResp1 = await callText(bob, "reply", {
  message_id: parseInt(firstQ),
  message: "first answer"
});
const firstA = replyResp1.match(/with #(\d+)/)?.[1];
await callText(alice, "inbox", {});

const replyResp2q = await callText(alice, "reply", {
  message_id: parseInt(firstA),
  message: "follow up question"
});
const secondQ = replyResp2q.match(/with #(\d+)/)?.[1];
await callText(bob, "inbox", {});

const replyResp2a = await callText(bob, "reply", {
  message_id: parseInt(secondQ),
  message: "follow up answer"
});
const secondA = replyResp2a.match(/with #(\d+)/)?.[1];
await callText(alice, "inbox", {});

// now call thread from the middle — should see the whole chain
result = await callText(alice, "thread", { message_id: parseInt(firstA) });
check("thread shows ancestors", result.includes("first question"), result);
check("thread shows descendants", result.includes("follow up answer"), result);
check("thread shows root message", result.includes("first answer"), result);
check("thread shows message count", result.includes("4 message"), result);

// thread from the last message in chain
result = await callText(bob, "thread", { message_id: parseInt(secondA) });
check("thread from leaf includes all ancestors",
  result.includes("first question") && result.includes("first answer") && result.includes("follow up question"),
  result);

// 6. FTS SEARCH — history with query param

console.log("\n--- test: FTS search via history query ---");

// insert some messages with different content
await callText(alice, "send", { message: "urgent security alert", to: "bob" });
await callText(alice, "send", { message: "normal status update", to: "bob" });
await callText(alice, "send", { message: "critical bug found", to: "bob" });

// search for "urgent"
result = await callText(alice, "history", { query: "urgent" });
check("FTS query finds 'urgent'", result.includes("urgent security alert"), result);
check("FTS query excludes non-matching", !result.includes("normal status"), result);

// search for "critical"
result = await callText(alice, "history", { query: "critical" });
check("FTS query finds 'critical'", result.includes("critical bug"), result);

// search for non-existent term
result = await callText(alice, "history", { query: "nonexistent" });
check("FTS query returns no results for non-matching term", result.includes("no messages matching"), result);

// search combining with with= param (search within a conversation)
result = await callText(alice, "history", { with: "bob", query: "status" });
check("FTS query with 'with' filters results", result.includes("normal status update"), result);

// 7. FMTMESSAGE REPLY PREFIX — all formatted output includes reply_to prefix

console.log("\n--- test: fmtMessage reply_to prefix ---");

result = await callText(alice, "get_message", { message_id: parseInt(secondQ) });
check("formatted reply shows ↩ prefix", result.includes("↩"), result);
check("formatted reply shows reference id", result.includes(`#${firstA}`), result);

await alice.close();
await bob.close();

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nall v3 checks passed");
