// R3: typed message envelope (type + payload, inbox type-filter) and TTL/[STALE].
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const dir = mkdtempSync(join(tmpdir(), "intercom-r3-"));
const DB = join(dir, "test.db");
const SERVER = new URL("../server.js", import.meta.url).pathname;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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

const alpha = await connect("alpha");
const beta = await connect("beta");
await call(alpha, "join", { name: "alpha" });
await call(beta, "join", { name: "beta" });

// ── typed envelope ──────────────────────────────────────────────────────────
console.log("=== typed envelope (type + payload + inbox type-filter) ===");
await call(alpha, "send", { to: "beta", message: "do it", type: "task", payload: { job: "build", n: 3 } });
await call(alpha, "send", { to: "beta", message: "fyi ok", type: "status" });

let c = await call(beta, "inbox", { type: "task", unread_count: true });
check("type-filter counts only task", /^1 unread/.test(c), c);
c = await call(beta, "inbox", { type: "status", unread_count: true });
check("type-filter counts only status", /^1 unread/.test(c), c);
c = await call(beta, "inbox", { type: "ghost", unread_count: true });
check("type-filter with no matches = 0", /^0 unread/.test(c), c);

const taskView = await call(beta, "inbox", { type: "task" }); // marks the task read
check("inbox shows the <task> type tag", /<task>/.test(taskView), taskView);
check("inbox shows the payload", /payload:/.test(taskView) && /build/.test(taskView), taskView);
check("status stays unread after fetching only task", /^1 unread/.test(await call(beta, "inbox", { unread_count: true })), "");
await call(beta, "inbox", {}); // drain the status message

// ── TTL / [STALE] ─────────────────────────────────────────────────────────────
console.log("\n=== TTL / [STALE] ===");
const sent = await call(alpha, "send", { to: "beta", message: "expires soon", ttl_seconds: 1 });
check("send reports the ttl", /\[ttl:1s\]/.test(sent), sent);
check("live before expiry (counts as unread)", /^1 unread/.test(await call(beta, "inbox", { unread_count: true })), "");
await sleep(1700);
check("dropped from unread after expiry", /^0 unread/.test(await call(beta, "inbox", { unread_count: true })), "");
const hist = await call(beta, "history", {});
check("history tags the expired message [STALE]", /\[STALE\]/.test(hist) && /expires soon/.test(hist), hist);

await alpha.close();
await beta.close();
rmSync(dir, { recursive: true, force: true });
if (failed) { console.error(`\n${failed} check(s) failed`); process.exit(1); }
console.log("\nall checks passed");
