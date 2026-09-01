"use strict";

// Runs on npm install. Fetching the binary here rather than on first run keeps
// the failure where it can be read: an install that cannot reach the release
// assets should say so now, not halfway through `npx resonate-cli dev`.

const { download, version } = require("./install.js");

download()
  .then((binary) => {
    console.log(`resonate ${version} installed to ${binary}`);
  })
  .catch((err) => {
    console.error(`\nFailed to install the resonate ${version} binary.\n`);
    console.error(`  ${err.message}\n`);
    console.error("Workarounds:");
    console.error(
      "  - point RESONATE_DOWNLOAD_BASE_URL at a mirror of the release assets"
    );
    console.error(
      "  - download the binary yourself and set RESONATE_BINARY_PATH to it"
    );
    console.error(
      "  - install with Homebrew instead: brew install resonatehq/tap/resonate\n"
    );
    console.error(
      "Releases: https://github.com/resonatehq/resonate/releases/tag/v" + version
    );
    process.exit(1);
  });
