// Smart stale-ask: asking a provably-stale recipient (idle 5+ min) returns fast
// after a short grace window instead of blocking the full timeout. The question is
// still queued and the answer still lands in the recipient's inbox — we just don't
// stall the caller. Live recipients are unaffected (block the full timeout as before).
// Uses a temp DB (never touches the live one). Topology mirrors v3-features.test.js.
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const dir = mkdtempSync(join(tmpdir(), "intercom-stale-ask-"));
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
const callText = async (client, name, args = {}) =>
  (await client.callTool({ name, arguments: args })).content[0].text;

let failed = 0;
const check = (label, cond, detail = "") => {
  if (cond) console.log(`  ok   ${label}`);
  else { failed++; console.error(`  FAIL ${label}\n       got: ${detail}`); }
};

const alpha = await connect("alpha");
const beta = await connect("beta");
await callText(alpha, "join", { name: "alpha", role: "stale-ask-test" });
await callText(beta, "join", { name: "beta" });

// --- backdate beta's last_seen to 10 min ago -> "stale" (PID alive, so still online) ---
const raw = new DatabaseSync(DB);
raw.prepare("UPDATE agents SET last_seen = ? WHERE name = ?")
  .run(new Date(Date.now() - 10 * 60 * 1000).toISOString(), "beta");
raw.close();

// --- alpha asks the STALE beta with a generous 30s timeout: should return ~grace, not 30s ---
const t0 = Date.now();
const out = await callText(alpha, "ask", { to: "beta", question: "you around?", timeout_seconds: 30 });
const elapsed = Date.now() - t0;

check("stale recipient: returns fast (<12s, not the full 30s)", elapsed < 12000, `elapsed ${elapsed}ms`);
check("stale recipient: honors the grace window (>=4s, didn't no-op)", elapsed >= 4000, `elapsed ${elapsed}ms`);
check("stale recipient: output notes recipient was stale", /stale/i.test(out), out);
check("stale recipient: question still queued", /queued/i.test(out), out);
check("stale recipient: references the question #id", /#\d+/.test(out), out);

// --- the queued question is still deliverable: it reaches beta's inbox ---
const betaInbox = await callText(beta, "inbox", {});
check("queued question reaches beta's inbox", /you around\?/.test(betaInbox), betaInbox);

// --- CONTROL: a LIVE recipient is NOT grace-capped — blocks the full (short) timeout, no stale note ---
await callText(beta, "who", {}); // beta makes a call -> last_seen refreshes -> live again
const t1 = Date.now();
const out2 = await callText(alpha, "ask", { to: "beta", question: "still there?", timeout_seconds: 2 });
const elapsed2 = Date.now() - t1;
check("live recipient: blocks the full (short) timeout (~2s)", elapsed2 >= 1800, `elapsed ${elapsed2}ms`);
check("live recipient: gets NO stale note", !/\bstale\b/i.test(out2), out2);

// --- teardown ---
try { await alpha.close(); await beta.close(); } catch {}
rmSync(dir, { recursive: true, force: true });
console.log(failed ? `\n${failed} check(s) FAILED` : "\nall ok");
process.exit(failed ? 1 : 0);
