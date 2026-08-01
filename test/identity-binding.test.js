// Test: identity binding — "who am I" must come from the SERVER, not a static argv
// string. The watcher is bound to a session (--server-pid), so a mid-session rename
// follows automatically instead of leaving the watcher polling an abandoned name.
//
// Hermetic: temp DB + temp INTERCOM_RUN_DIR, so it never touches the live fleet's
// ~/.intercom (session files, watcher pidfiles) or ~/.local/share/intercom.
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";

const dir = mkdtempSync(join(tmpdir(), "intercom-binding-"));
const DB = join(dir, "test.db");
const RUN_DIR = join(dir, "run");
const SERVER = new URL("../server.js", import.meta.url).pathname;
const WATCHER = new URL("../monitor-watcher.js", import.meta.url).pathname;
const env = { ...process.env, INTERCOM_DB: DB, INTERCOM_RUN_DIR: RUN_DIR, TMUX: "", TMUX_PANE: "" };

async function connect(label) {
  const client = new Client({ name: `test-${label}`, version: "0.0.0" });
  await client.connect(new StdioClientTransport({ command: process.execPath, args: [SERVER], env }));
  return client;
}
const callText = async (client, name, args = {}) => {
  const res = await client.callTool({ name, arguments: args });
  return res.content[0].text;
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Direct DB peeks (read-only) so assertions don't depend on message-id arithmetic.
const peek = (sql, ...params) => {
  const d = new DatabaseSync(DB);
  const row = d.prepare(sql).get(...params);
  d.close();
  return row;
};
const lastMessageId = () => peek("SELECT MAX(id) AS id FROM messages").id;

let failed = 0;
const check = (label, cond, detail) => {
  if (cond) console.log(`  ok   ${label}`);
  else { failed++; console.error(`  FAIL ${label}\n       got: ${detail}`); }
};

// A watcher we can read incrementally while it keeps running.
function startWatcher(args) {
  const proc = spawn(process.execPath, [WATCHER, ...args], { env });
  let out = "";
  proc.stdout.on("data", (d) => { out += d.toString(); });
  proc.stderr.on("data", (d) => { out += d.toString(); });
  return { proc, read: () => out, stop: () => proc.kill("SIGKILL") };
}

console.log("=== watcher bound to a session follows a rename ===");
const seat = await connect("seat");
await callText(seat, "join", { name: "alpha" });

const seatPid = peek("SELECT pid FROM agents WHERE name = ?", "alpha")?.pid;
check("seat pid recorded in agents", Number.isInteger(seatPid), String(seatPid));

const sender = await connect("sender");
await callText(sender, "join", { name: "sender" });

const w = startWatcher(["--server-pid", String(seatPid), "--me", "alpha", "--interval", "1"]);
await sleep(1200);

await callText(sender, "send", { to: "alpha", message: "before-rename" });
const idBefore = lastMessageId();
await sleep(2500);
check("watcher reports mail for the pre-rename identity",
  w.read().includes(`#${idBefore}`), w.read());

// Rename mid-session. The watcher's argv still says "alpha"; only the server knows.
await callText(seat, "join", { name: "beta" });
await sleep(1500);
await callText(sender, "send", { to: "beta", message: "after-rename" });
const idAfter = lastMessageId();
await sleep(2500);
check("watcher followed the rename (sees mail addressed to beta, argv says alpha)",
  w.read().includes(`#${idAfter}`), w.read());
w.stop();

// Old callers (--me only, no --server-pid) must behave exactly as before: pinned name.
console.log("\n=== --me only stays pinned (backward compat) ===");
const w2 = startWatcher(["--me", "sender", "--interval", "1"]);
await sleep(1200);
await callText(seat, "send", { to: "sender", message: "pinned-delivery" });
const idPinned = lastMessageId();
await sleep(2500);
check("legacy --me watcher still delivers", w2.read().includes(`#${idPinned}`), w2.read());
w2.stop();

await seat.close();
await sender.close();
await sleep(200);
rmSync(dir, { recursive: true, force: true });
if (failed) { console.error(`\n${failed} check(s) failed`); process.exit(1); }
console.log("\nall identity-binding checks passed");
process.exit(0);
