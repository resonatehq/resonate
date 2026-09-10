// New release helper, mirroring resonate-sdk-py's scripts/new-release.py.
// Opens the GitHub "new release" page with the root package's version as the
// tag (vX.Y.Z). One release publishes the whole monorepo (see cd.yml).
//
// Usage: npx tsx scripts/release.ts

import { execFile } from "node:child_process";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const REPO = "https://github.com/resonatehq/resonate-sdk-ts";

const { version } = JSON.parse(readFileSync(resolve(process.cwd(), "package.json"), "utf-8")) as { version?: string };
if (!version) {
  console.error('❌ package.json is missing "version".');
  process.exit(1);
}

const tag = `v${version}`;
const url = `${REPO}/releases/new?${new URLSearchParams({ tag, title: tag })}`;

console.log(`🔗 Opening: ${url}`);
// Native OS opener — no dependency (macOS: open, Windows: start, Linux: xdg-open).
const opener = process.platform === "darwin" ? "open" : process.platform === "win32" ? "start" : "xdg-open";
execFile(opener, [url], (err) => {
  if (err) console.warn(`⚠️  Could not launch a browser (${err.message}). Open the URL above.`);
});
