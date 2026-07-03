// Regression test for monitor-watcher.js argument parsing.
//
// The bare positional form `monitor-watcher.js <name>` — which CHARTER.md and the Republic
// agent boot docs actually tell agents to use — used to exit(2) because only `--me <name>`
// was accepted. This locks in that BOTH forms work, that a positional name coexists with
// --interval, and that a truly missing name still errors loudly with exit 2 + usage.
//
// How the accepted cases are checked: the watcher is an infinite poll loop, so an accepted
// name never self-exits. We SIGKILL it at the timeout — status === null (force-killed, i.e.
// it was still running) proves the name was accepted. A rejected name self-exits 2 first.
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const WATCHER = new URL("../monitor-watcher.js", import.meta.url).pathname;
const testDir = mkdtempSync(join(tmpdir(), "intercom-watcher-args-"));
const DB = join(testDir, "test.db"); // hermetic: never touch the live bus

// Base env with INTERCOM_NAME removed so only the CLI args decide the name.
const baseEnv = () => {
  const e = { ...process.env, INTERCOM_DB: DB };
  delete e.INTERCOM_NAME;
  return e;
};

let failed = 0;
const check = (label, cond, detail = "") => {
  if (cond) console.log(`  ok   ${label}`);
  else { failed++; console.error(`  FAIL ${label}\n       ${detail}`); }
};

// Accepted: name resolves -> poll loop -> force-killed at timeout (status null, signal SIGKILL).
function runAccepted(label, args) {
  const r = spawnSync(process.execPath, [WATCHER, ...args],
    { env: baseEnv(), timeout: 1200, killSignal: "SIGKILL", encoding: "utf-8" });
  check(label, r.status === null && r.signal === "SIGKILL",
    `expected still-running@timeout, got status=${r.status} signal=${r.signal} stderr=${(r.stderr || "").trim()}`);
}

runAccepted("bare positional name is accepted (the regression)", ["zzz-selftest"]);
runAccepted("--me <name> still accepted (backward compat)", ["--me", "zzz-selftest"]);
runAccepted("positional name coexists with --interval", ["zzz-selftest", "--interval", "1"]);

// Rejected: no name anywhere -> self-exits 2 before the timeout, with usage on stderr.
const noName = spawnSync(process.execPath, [WATCHER],
  { env: baseEnv(), timeout: 3000, encoding: "utf-8" });
check("missing name exits 2", noName.status === 2, `got status=${noName.status} signal=${noName.signal}`);
check("missing-name usage documents the bare form", /<name>/.test(noName.stderr || ""), noName.stderr);

rmSync(testDir, { recursive: true, force: true });
if (failed) { console.error(`\n${failed} check(s) failed`); process.exit(1); }
console.log("\nall monitor-watcher arg tests passed");
process.exit(0);
