// Test: R4 persistent identity.
// Covers: (a) a fresh process REATTACHES its offline identity, restoring role/topics;
// (b) send() to an OFFLINE identity queues and delivers on reattach;
// (c) the cross-scope accident-guardrail suffixes an impostor, and the token overrides it;
// (d) who include_offline surfaces offline identities that the default who hides.
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { mkdtempSync, rmSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";

const dir = mkdtempSync(join(tmpdir(), "intercom-identity-"));
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
  else { failed++; console.error(`  FAIL ${label}\n       got: ${detail}`); }
};

// ── (a) reattach restores role/topics after the original process goes away ───
console.log("=== (a) reattach restores a durable identity ===");
const a = await connect("a");
let out = await callText(a, "join", {
  name: "judge", role: "the decider", topics: ["rulings"],
});
check("judge created as a new identity", out.includes('joined as "judge"') && out.includes("new identity"), out);
await a.close(); // process gone → judge is now OFFLINE (pid dead)

// ── (b) a message sent while judge is offline must queue ─────────────────────
console.log("\n=== (b) send to an offline identity queues ===");
const s = await connect("s");
await callText(s, "join", { name: "sender" });
out = await callText(s, "send", { to: "judge", message: "ping-while-offline" });
check("send to offline judge reports it queued", /offline/i.test(out) && /queued/i.test(out), out);

// reattach judge from a fresh process (same cwd as creation → no token needed)
const b = await connect("b");
out = await callText(b, "join", { name: "judge" });
check("judge reattaches (not a fresh blank identity)", out.includes("reattached"), out);
check("reattach restores role", out.includes("the decider"), out);
check("reattach restores topics", /rulings/.test(out), out);
out = await callText(b, "inbox", {});
check("the offline-queued message is delivered on reattach", out.includes("ping-while-offline"), out);
await b.close();

// ── (c) cross-scope guardrail: impostor is suffixed; token overrides ─────────
console.log("\n=== (c) cross-scope guardrail + token override ===");
const d = await connect("d");
out = await callText(d, "join", { name: "ghost", role: "owner", memory_scope: "/scope/A" });
const token = (out.match(/identity token: ([0-9a-f]+)/) || [])[1];
check("ghost created with an explicit foreign scope + token issued", !!token, out);
await d.close(); // ghost offline; its scope is "/scope/A", NOT this test's cwd

// a different session, no token, wrong cwd → must NOT become ghost
const e = await connect("e");
out = await callText(e, "join", { name: "ghost" });
check("cross-scope impostor is suffixed, not reattached", out.includes('joined as "ghost-2"'), out);
check("impostor did NOT inherit ghost's role", !out.includes("owner"), out);
await e.close();

// the real owner presents the token → reattaches ghost even from the wrong cwd
const f = await connect("f");
out = await callText(f, "join", { name: "ghost", token });
check("token override reattaches ghost cross-scope", out.includes('joined as "ghost"') && out.includes("reattached"), out);
check("token reattach restores role", out.includes("owner"), out);

// ── (d) who include_offline surfaces offline identities ──────────────────────
console.log("\n=== (d) who include_offline ===");
out = await callText(f, "who", {}); // default = online only
check("default who hides offline judge", !out.includes("judge"), out);
out = await callText(f, "who", { include_offline: true });
check("who include_offline surfaces offline judge", out.includes("judge"), out);
await f.close();

// ── (e) legacy (pre-R4) identity: reattaches AND is upgraded with a guardrail ─
console.log("\n=== (e) legacy identity upgrade on reattach ===");
{
  // a pre-R4 row: role/topics but NO memory_scope, NO token, and a dead pid → offline
  const ldb = new DatabaseSync(DB);
  const ts = new Date().toISOString();
  ldb.prepare(
    `INSERT INTO agents (name, pid, pid_start, cwd, role, joined_at, last_seen, topics)
     VALUES (?, ?, NULL, ?, ?, ?, ?, ?)`
  ).run("oldbot", 999999, "/old/cwd", "legacy role", ts, ts, JSON.stringify(["legacy"]));
  ldb.close();
}
const g = await connect("g");
out = await callText(g, "join", { name: "oldbot" }); // no token, cwd ≠ /old/cwd, but legacy-open
check("legacy offline identity reattaches (backward-compat open)", out.includes('joined as "oldbot"') && out.includes("reattached"), out);
check("legacy identity is upgraded with a fresh token", /token [0-9a-f]+/.test(out) && /predated/.test(out), out);
check("legacy reattach still restores role", out.includes("legacy role"), out);
await g.close();
{
  const ldb = new DatabaseSync(DB);
  const row = ldb.prepare("SELECT memory_scope, token FROM agents WHERE name = ?").get("oldbot");
  check("legacy identity now carries a scope + token (guardrail active)", !!row.memory_scope && !!row.token, JSON.stringify(row));
  ldb.close();
}

await s.close();
rmSync(dir, { recursive: true, force: true });
if (failed) { console.error(`\n${failed} check(s) failed`); process.exit(1); }
console.log("\nall checks passed");
