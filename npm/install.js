"use strict";

// Resonate is a single Rust binary. This package does not reimplement any of
// it — it downloads the release artifact built by .github/workflows/cd.yml and
// unpacks it, so that a JavaScript developer can run
//
//   npx resonate-cli@latest dev
//
// without a Homebrew tap, a Docker daemon, or a Rust toolchain. npm is used as
// a distribution channel, nothing more.

const crypto = require("crypto");
const fs = require("fs");
const https = require("https");
const os = require("os");
const path = require("path");
const { spawnSync } = require("child_process");

const { version } = require("./package.json");

// The CD pipeline publishes resonate_<os>_<arch>.tar.gz for these four targets
// and no others. A platform absent from this table has no artifact to fetch,
// which is a clearer thing to say than a 404 from the releases page.
const TARGETS = {
  "darwin arm64": "darwin_aarch64",
  "darwin x64": "darwin_x86_64",
  "linux arm64": "linux_aarch64",
  "linux x64": "linux_x86_64",
};

const RELEASES = "https://github.com/resonatehq/resonate/releases/download";

// Both escape hatches exist for the same situation: a machine that cannot
// reach GitHub. One points at a mirror of the release assets, the other skips
// the download entirely and uses a binary that is already on disk.
const baseUrl = process.env.RESONATE_DOWNLOAD_BASE_URL || RELEASES;
const override = process.env.RESONATE_BINARY_PATH;

const binaryPath = path.join(__dirname, "bin", "resonate");

function target() {
  const key = `${process.platform} ${process.arch}`;
  const value = TARGETS[key];
  if (!value) {
    throw new Error(
      `no Resonate binary is published for ${key}.\n` +
        `Published targets: ${Object.keys(TARGETS).join(", ")}.\n` +
        `Build from source (https://github.com/resonatehq/resonate) and set ` +
        `RESONATE_BINARY_PATH to the result.`
    );
  }
  return value;
}

// GitHub answers a release download with a redirect to object storage, and
// object storage answers with another one often enough to be worth following.
function fetch(url, redirects = 5) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { "user-agent": `resonate-cli/${version}` } }, (res) => {
        const { statusCode, headers } = res;
        if (statusCode >= 300 && statusCode < 400 && headers.location) {
          res.resume();
          if (redirects === 0) {
            reject(new Error(`too many redirects fetching ${url}`));
            return;
          }
          resolve(fetch(new URL(headers.location, url).toString(), redirects - 1));
          return;
        }
        if (statusCode !== 200) {
          res.resume();
          reject(new Error(`GET ${url} responded ${statusCode}`));
          return;
        }
        const chunks = [];
        res.on("data", (chunk) => chunks.push(chunk));
        res.on("end", () => resolve(Buffer.concat(chunks)));
        res.on("error", reject);
      })
      .on("error", reject);
  });
}

// The release carries a .sha256 next to every tarball; ignoring it would leave
// the install trusting whatever came back over the wire.
function verify(archive, checksumFile, name) {
  const expected = checksumFile.toString("utf8").trim().split(/\s+/)[0];
  const actual = crypto.createHash("sha256").update(archive).digest("hex");
  if (expected !== actual) {
    throw new Error(
      `checksum mismatch for ${name}\n  expected ${expected}\n  actual   ${actual}`
    );
  }
}

// Every target is macOS or Linux, so tar is already there. Unpacking it in
// process would mean either a dependency or a tar parser of our own, and this
// package is meant to stay small enough to read in one sitting.
function untar(archive, name) {
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "resonate-"));
  const tarball = path.join(scratch, name);
  try {
    fs.writeFileSync(tarball, archive);
    const result = spawnSync("tar", ["-xzf", tarball, "-C", scratch], {
      stdio: ["ignore", "ignore", "pipe"],
    });
    if (result.error) throw result.error;
    if (result.status !== 0) {
      throw new Error(`tar exited ${result.status}: ${result.stderr}`);
    }
    fs.mkdirSync(path.dirname(binaryPath), { recursive: true });
    // Rename would fail across filesystems, and the tmpdir often is one.
    fs.copyFileSync(path.join(scratch, "resonate"), binaryPath);
    fs.chmodSync(binaryPath, 0o755);
  } finally {
    fs.rmSync(scratch, { recursive: true, force: true });
  }
}

async function download() {
  const name = `resonate_${target()}.tar.gz`;
  const url = `${baseUrl}/v${version}/${name}`;

  const [archive, checksum] = await Promise.all([
    fetch(url),
    fetch(`${url}.sha256`),
  ]);

  verify(archive, checksum, name);
  untar(archive, name);

  return binaryPath;
}

// Called from postinstall, and again from the shim in case the install scripts
// were skipped — npm --ignore-scripts and some CI defaults do exactly that,
// and a CLI that only fails at run time is worse than one that downloads late.
async function ensureBinary() {
  if (override) return override;
  if (fs.existsSync(binaryPath)) return binaryPath;
  return download();
}

module.exports = { binaryPath, download, ensureBinary, version };
