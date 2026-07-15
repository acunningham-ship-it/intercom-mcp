// Integration test: respond_to_broadcast + collect_responses (Lane B, Round 2).
// Two workers (bob, carol) respond to a coordinator's (alice) broadcast.
// Verifies: responses are threaded, collect returns them all, inbox stays clean.
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const dir = mkdtempSync(join(tmpdir(), "intercom-rc-test-"));
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

const alice = await connect("alice");
const bob   = await connect("bob");
const carol = await connect("carol");

await callText(alice, "join", { name: "alice", role: "coordinator" });
await callText(bob,   "join", { name: "bob",   role: "worker" });
await callText(carol, "join", { name: "carol", role: "worker" });

// alice broadcasts an assignment
let out = await callText(alice, "send", { message: "what's 2+2?" });
check("broadcast sent", out.includes("broadcast"), out);
const bcastId = Number(out.match(/#(\d+)/)[1]);

// bob responds via respond_to_broadcast
out = await callText(bob, "respond_to_broadcast", { message_id: bcastId, body: "4" });
check("bob respond_to_broadcast returns confirmation", out.includes("response") && out.includes(`reply_to #${bcastId}`), out);
const bobRespId = Number(out.match(/#(\d+)/)[1]);

// carol responds via respond_to_broadcast
out = await callText(carol, "respond_to_broadcast", { message_id: bcastId, body: "still 4" });
check("carol respond_to_broadcast returns confirmation", out.includes("response") && out.includes(`reply_to #${bcastId}`), out);

// collect_responses returns both
out = await callText(alice, "collect_responses", { message_id: bcastId });
check("collect_responses returns 2 responses", out.includes("2 response"), out);
check("collect_responses includes bob's answer", out.includes("bob: 4"), out);
check("collect_responses includes carol's answer", out.includes("carol: still 4"), out);

// collect_responses excludes unrelated messages
let out2 = await callText(alice, "send", { message: "unrelated broadcast" });
const unrelatedId = Number(out2.match(/#(\d+)/)[1]);
await callText(bob, "respond_to_broadcast", { message_id: unrelatedId, body: "unrelated response" });
out = await callText(alice, "collect_responses", { message_id: bcastId });
check("collect_responses for original id still only 2 (excludes unrelated)", out.includes("2 response"), out);

// responses do NOT appear in alice's inbox (unread.js filters kind='response')
out = await callText(alice, "inbox", {});
check("alice inbox does not include responses (stays uncluttered)", !out.includes("bob: 4") && !out.includes("carol: still 4"), out);

// respond_to_broadcast on a directed (non-broadcast) message is rejected
out = await callText(alice, "send", { message: "directed", to: "bob" });
const directedId = Number(out.match(/#(\d+)/)[1]);
out = await callText(carol, "respond_to_broadcast", { message_id: directedId, body: "nope" });
check("respond_to_broadcast on directed message is rejected", out.includes("not a broadcast"), out);

// respond_to_broadcast on missing id is rejected
out = await callText(bob, "respond_to_broadcast", { message_id: 999999, body: "ghost" });
check("respond_to_broadcast on missing id is rejected", out.includes("no message"), out);

// collect_responses on message with no responses yet returns 'no responses'
out = await callText(alice, "send", { message: "empty broadcast" });
const emptyId = Number(out.match(/#(\d+)/)[1]);
out = await callText(alice, "collect_responses", { message_id: emptyId });
check("collect_responses on broadcast with no responses says 'no responses'", out.includes("no responses"), out);

// Close the clients (shuts down the spawned server children) + drop the temp dir, else the
// open stdio pipes keep the event loop alive and the process hangs AFTER passing — which
// stalled the whole `npm test` && chain at this file.
await alice.close();
await bob.close();
await carol.close();
rmSync(dir, { recursive: true, force: true });

if (failed) {
  console.error(`\n${failed} check(s) failed.`);
  process.exit(1);
} else {
  console.log("\nall checks passed.");
}
