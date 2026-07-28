// R3 (part 2): ask_async (non-blocking question) + wait_for_any (first answer wins).
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const dir = mkdtempSync(join(tmpdir(), "intercom-async-"));
const DB = join(dir, "test.db");
const SERVER = new URL("../server.js", import.meta.url).pathname;

async function connect(label) {
  const client = new Client({ name: `test-${label}`, version: "0.0.0" });
  await client.connect(new StdioClientTransport({
    command: process.execPath, args: [SERVER],
    env: { ...process.env, INTERCOM_DB: DB, TMUX: "", TMUX_PANE: "" },
  }));
  return client;
}
const call = async (c, name, args = {}) => (await c.callTool({ name, arguments: args })).content[0].text;
let failed = 0;
const check = (label, cond, detail) => {
  if (cond) console.log(`  ok   ${label}`);
  else { failed++; console.error(`  FAIL ${label}\n       got: ${detail}`); }
};
const qidOf = (s) => Number((s.match(/asked #(\d+)/) || [])[1]);

const alpha = await connect("alpha");
const beta = await connect("beta");
const gamma = await connect("gamma");
await call(alpha, "join", { name: "alpha" });
await call(beta, "join", { name: "beta" });
await call(gamma, "join", { name: "gamma" });

console.log("=== ask_async is non-blocking + returns a qid ===");
const t0 = Date.now();
const a1 = await call(alpha, "ask_async", { to: "beta", question: "beta, ready?" });
const q1 = qidOf(a1);
const a2 = await call(alpha, "ask_async", { to: "gamma", question: "gamma, ready?" });
const q2 = qidOf(a2);
check("ask_async returned immediately (no block)", Date.now() - t0 < 3000, `${Date.now() - t0}ms`);
check("ask_async #1 gave a question id", Number.isInteger(q1) && q1 > 0, a1);
check("ask_async #2 gave a distinct id", Number.isInteger(q2) && q2 !== q1, a2);

console.log("\n=== wait_for_any returns the first answer among the fanned-out ids ===");
await call(beta, "inbox", {}); // beta sees the question
await call(beta, "reply", { message_id: q1, message: "yep beta is ready" });
const got = await call(alpha, "wait_for_any", { question_ids: [q1, q2], timeout_seconds: 10 });
check("wait_for_any returns beta's answer", /yep beta is ready/.test(got), got);
check("wait_for_any names the answered question", new RegExp(`#${q1}`).test(got), got);

console.log("\n=== wait_for_any times out cleanly when nobody answers ===");
const a3 = await call(alpha, "ask_async", { to: "beta", question: "no one will answer this" });
const q3 = qidOf(a3);
const t1 = Date.now();
const timedOut = await call(alpha, "wait_for_any", { question_ids: [q3], timeout_seconds: 1 });
check("times out after ~the timeout (not forever)", Date.now() - t1 < 4000, `${Date.now() - t1}ms`);
check("timeout message says still queued", /no answer/.test(timedOut) && /queued/.test(timedOut), timedOut);

await alpha.close(); await beta.close(); await gamma.close();
rmSync(dir, { recursive: true, force: true });
if (failed) { console.error(`\n${failed} check(s) failed`); process.exit(1); }
console.log("\nall checks passed");
