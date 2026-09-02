#!/usr/bin/env node
"use strict";

// A shim, not a CLI. Everything typed after `resonate` is handed to the Rust
// binary untouched, and its exit status is handed back. The indirection buys
// one thing: bin/resonate.js exists in the published tarball, so npm can link
// the command before the install script has fetched anything.

const { spawn } = require("child_process");
const { ensureBinary } = require("../install.js");

// A terminal delivers Ctrl-C to the whole process group, so the child sees it
// with no help from us — but `kill <pid>`, a container stop, and a process
// supervisor all signal this process alone, and a server that outlives its
// wrapper is a stray port and a held database. Forward, then wait for the
// child to finish shutting down rather than exiting out from under it.
const FORWARDED = ["SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT"];

ensureBinary()
  .then((binary) => {
    const child = spawn(binary, process.argv.slice(2), { stdio: "inherit" });

    for (const signal of FORWARDED) {
      process.on(signal, () => {
        try {
          child.kill(signal);
        } catch {
          // The child won the race and is already gone; its exit handler
          // below is what ends this process.
        }
      });
    }

    child.on("error", (err) => {
      console.error(`resonate: failed to start ${binary}: ${err.message}`);
      process.exit(1);
    });

    // Exit the way the child did, so that `resonate ... || echo failed` and a
    // supervisor watching for a signal death both see the truth.
    child.on("exit", (code, signal) => {
      for (const s of FORWARDED) process.removeAllListeners(s);
      if (signal) {
        process.kill(process.pid, signal);
        return;
      }
      process.exit(code === null ? 1 : code);
    });
  })
  .catch((err) => {
    console.error(`resonate: ${err.message}`);
    process.exit(1);
  });
