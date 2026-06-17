// r2-a tests: claim (atomic first-N-win) + assign/tasks round-trip.
// Uses a temp DB; never touches the live intercom.
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const dir = mkdtempSync(join(tmpdir(), "intercom-r2a-test-"));
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
// SETUP

const coord  = await connect("coord");
const agent1 = await connect("agent1");
const agent2 = await connect("agent2");
const agent3 = await connect("agent3");

await callText(coord,  "join", { name: "coord" });
await callText(agent1, "join", { name: "agent1" });
await callText(agent2, "join", { name: "agent2" });
await callText(agent3, "join", { name: "agent3" });

// -----------------------------------------------------------------------
// 1. send with max_claims

console.log("\n=== r2-a: claim — first-N succeed, N+1 gets full ===");

let out;

// coord broadcasts a claimable task (max 2 slots)
out = await callText(coord, "send", { message: "do the thing", max_claims: 2 });
check("send with max_claims confirms claimable", out.includes("[claimable:2]"), out);
check("send shows claim hint with message_id", out.includes("claim(message_id:"), out);
const msgId = parseInt(out.match(/claim\(message_id:\s*(\d+)\)/)?.[1] ?? "0");
check("extracted message_id is valid", msgId > 0, `msgId=${msgId}`);

// -----------------------------------------------------------------------
// 2. claim — first two succeed

out = await callText(agent1, "claim", { message_id: msgId });
check("agent1 claims slot 1", out.includes("claimed") && out.includes("slot 1/2"), out);

out = await callText(agent2, "claim", { message_id: msgId });
check("agent2 claims slot 2", out.includes("claimed") && out.includes("slot 2/2"), out);

// -----------------------------------------------------------------------
// 3. N+1 gets "full"

out = await callText(agent3, "claim", { message_id: msgId });
check("agent3 gets full (N+1 rejected)", out.includes("full"), out);
check("full message names the claimers", out.includes("agent1") && out.includes("agent2"), out);

// -----------------------------------------------------------------------
// 4. idempotent — already-claimed agent gets "already claimed", not a new slot

out = await callText(agent1, "claim", { message_id: msgId });
check("agent1 reclaiming is idempotent (already claimed)", out.includes("already claimed"), out);
check("idempotent claim returns correct slot", out.includes("slot 1/2"), out);

// -----------------------------------------------------------------------
// 5. non-claimable message rejects claim

const plainId = parseInt(
  (await callText(coord, "send", { message: "plain broadcast" })).match(/sent #(\d+)/)?.[1] ?? "0"
);
check("extracted plain message id", plainId > 0, `plainId=${plainId}`);
out = await callText(agent3, "claim", { message_id: plainId });
check("claim on non-claimable message returns error", out.includes("not claimable"), out);

// -----------------------------------------------------------------------
// 6. claim on non-existent message

out = await callText(agent1, "claim", { message_id: 999999 });
check("claim on missing message_id returns error", out.includes("no message"), out);

// -----------------------------------------------------------------------
// 7. assign / tasks round-trip

console.log("\n=== r2-a: assign + tasks round-trip ===");

// tasks when empty
out = await callText(coord, "tasks", {});
check("tasks returns empty notice when no assignments", out.includes("no assignments"), out);

// assign two agents
out = await callText(coord, "assign", { agent: "agent1", label: "README draft" });
check("assign returns confirmation for agent1", out.includes("agent1") && out.includes("README draft"), out);

out = await callText(coord, "assign", { agent: "agent2", label: "homepage copy" });
check("assign returns confirmation for agent2", out.includes("agent2") && out.includes("homepage copy"), out);

// tasks shows both
out = await callText(coord, "tasks", {});
check("tasks shows agent1 assignment", out.includes("agent1") && out.includes("README draft"), out);
check("tasks shows agent2 assignment", out.includes("agent2") && out.includes("homepage copy"), out);
check("tasks shows count", out.includes("2"), out);

// -----------------------------------------------------------------------
// 8. assign upsert — updating an existing agent's label

out = await callText(coord, "assign", { agent: "agent1", label: "README v2" });
check("assign upsert returns confirmation with new label", out.includes("README v2"), out);

out = await callText(coord, "tasks", {});
check("tasks reflects updated label for agent1", out.includes("README v2"), out);
check("tasks does not show old label after upsert", !out.includes("README draft"), out);

// -----------------------------------------------------------------------
// 9. clear assignment

out = await callText(coord, "assign", { agent: "agent2", label: "" });
check("assign with empty label clears agent2", out.includes("cleared") && out.includes("agent2"), out);

out = await callText(coord, "tasks", {});
check("tasks no longer shows cleared agent", !out.includes("agent2"), out);
check("tasks still shows agent1 after clear", out.includes("agent1"), out);

// clear non-existent assignment
out = await callText(coord, "assign", { agent: "nobody", label: "" });
check("clearing non-existent assignment returns informative message", out.includes("no assignment"), out);

// -----------------------------------------------------------------------

await coord.close();
await agent1.close();
await agent2.close();
await agent3.close();
rmSync(dir, { recursive: true, force: true });

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nall r2-a checks passed");
