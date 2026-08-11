// Test: ensureJoined() reattaches a resumed session to ITS OWN durable identity when the
// cwd matches, instead of silently minting a sibling "hamtek-NNNN" identity that leaves a
// message addressed to the real name sitting unread forever. Root cause + fix: server.js
// ensureJoined() (the pre-join auto-provision path) — see judge intercom #5199/#5197.
//
// Drives the REAL ensureJoined()/signIn()/credentialOk() path by spawning the actual
// server.js against a throwaway DB fixture — NOT the live bus. Same harness pattern as
// identity-binding.test.js.
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { mkdtempSync, mkdirSync, rmSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";

const dir = mkdtempSync(join(tmpdir(), "intercom-reattach-"));
const DB = join(dir, "test.db");
const RUN_DIR = join(dir, "run");
const SERVER = new URL("../server.js", import.meta.url).pathname;
const env = { ...process.env, INTERCOM_DB: DB, INTERCOM_RUN_DIR: RUN_DIR, TMUX: "", TMUX_PANE: "" };

// Real, distinct cwds — the fix keys off process.cwd(), not a stand-in string, so these
// have to be directories that actually exist.
const dexCwd = join(dir, "dex-cwd"); mkdirSync(dexCwd);
const sharedCwd = join(dir, "shared-cwd"); mkdirSync(sharedCwd);
const emptyCwd = join(dir, "empty-cwd"); mkdirSync(emptyCwd);
const aCwd = join(dir, "a-cwd"); mkdirSync(aCwd);
const bCwd = join(dir, "b-cwd"); mkdirSync(bCwd);

async function connect(label, cwd) {
  const client = new Client({ name: `test-${label}`, version: "0.0.0" });
  await client.connect(new StdioClientTransport({ command: process.execPath, args: [SERVER], env, cwd }));
  return client;
}
const callText = async (client, name, args = {}) => {
  const res = await client.callTool({ name, arguments: args });
  return res.content[0].text;
};
const meOf = (whoText) => (whoText.match(/- (\S+) \(you\)/) || [])[1];
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const peek = (sql, ...params) => {
  const d = new DatabaseSync(DB);
  const rows = d.prepare(sql).all(...params);
  d.close();
  return rows;
};

let failed = 0;
const check = (label, cond, detail) => {
  if (cond) console.log(`  ok   ${label}`);
  else { failed++; console.error(`  FAIL ${label}\n       got: ${detail}`); }
};

// ── (a) unique offline match at this cwd → silent reattach, no sibling minted ──────────
console.log("=== unique offline match: reattaches to dex, not a fresh name ===");
{
  const first = await connect("dex-boot", dexCwd);
  await callText(first, "join", { name: "dex" });
  await first.close();
  await sleep(300); // let the exit handler mark it offline

  const resumed = await connect("dex-resume", dexCwd);
  const whoOut = await callText(resumed, "who", {}); // non-join tool first, like a real resumed session
  check("resumed session shows as dex, not a generic name", meOf(whoOut) === "dex", whoOut);
  check("no sibling identity minted for dex's cwd",
    peek("SELECT name FROM agents WHERE memory_scope = ?", dexCwd).length === 1,
    JSON.stringify(peek("SELECT name FROM agents WHERE memory_scope = ?", dexCwd)));
  await resumed.close();
}

// ── (b) two identities share a cwd → ambiguous, do NOT guess ───────────────────────────
console.log("\n=== ambiguous cwd (two identities): no false reattach ===");
{
  const s1 = await connect("alice-boot", aCwd);
  await callText(s1, "join", { name: "alice", memory_scope: sharedCwd });
  await s1.close();
  const s2 = await connect("bob-boot", bCwd);
  await callText(s2, "join", { name: "bob", memory_scope: sharedCwd });
  await s2.close();
  await sleep(300);

  const third = await connect("third", sharedCwd);
  const whoOut = await callText(third, "who", {});
  const assigned = meOf(whoOut);
  check("did not silently reattach to alice", assigned !== "alice", whoOut);
  check("did not silently reattach to bob", assigned !== "bob", whoOut);
  check("warning names both candidates", whoOut.includes("alice") && whoOut.includes("bob"), whoOut);
  check("alice is untouched (still offline, no pid stolen)",
    peek("SELECT left_at FROM agents WHERE name = ?", "alice")[0]?.left_at !== null,
    JSON.stringify(peek("SELECT left_at FROM agents WHERE name = ?", "alice")));
  await third.close();
}

// ── (c) no match at this cwd → loud generic provision ──────────────────────────────────
console.log("\n=== no match: loud auto-provision ===");
{
  const s = await connect("fresh", emptyCwd);
  const whoOut = await callText(s, "who", {});
  check("warning explains no match was found",
    whoOut.includes("no existing identity matches this cwd"), whoOut);
  check("still got a working generic identity", !!meOf(whoOut), whoOut);
  await s.close();
}

// ── (d) unique match but LIVE under another pid → don't seize, respect takeover ────────
console.log("\n=== live conflict: don't seize, warn instead ===");
{
  const liveCwd = join(dir, "live-cwd"); mkdirSync(liveCwd, { recursive: true });
  const holder = await connect("holder", liveCwd);
  await callText(holder, "join", { name: "carol" }); // stays open — carol is LIVE

  const other = await connect("other", liveCwd);
  const whoOut = await callText(other, "who", {});
  const assigned = meOf(whoOut);
  check("did not seize carol's live identity", assigned !== "carol", whoOut);
  check("warning explains the live conflict", whoOut.includes("carol") && whoOut.includes("online elsewhere"), whoOut);
  await other.close();
  await holder.close();
}

await sleep(200);
rmSync(dir, { recursive: true, force: true });
if (failed) { console.error(`\n${failed} check(s) failed`); process.exit(1); }
console.log("\nall reattach-fixture checks passed");
process.exit(0);
