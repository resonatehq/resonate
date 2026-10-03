# The browser image: rn8, Node, Playwright's Chromium, and guest.ts.
#
# Built from the repository root, since it needs both the server workspace (for
# rn8) and the TypeScript SDK:
#
#   docker build -f impl/sdk/ts/examples/sandbox/Dockerfile -t resonate-browser .

FROM rust:1 AS rn8
RUN rustup target add x86_64-unknown-linux-musl
COPY impl/server/core /src
RUN cargo build --release --manifest-path /src/Cargo.toml \
      -p resonate-sandbox-rn8 --bin rn8 --target x86_64-unknown-linux-musl

# Playwright's own image: Node, and the Chromium that playwright-core 1.56.1
# expects, already installed.
FROM mcr.microsoft.com/playwright:v1.56.1-noble
COPY --from=rn8 /src/target/x86_64-unknown-linux-musl/release/rn8 /usr/local/bin/rn8
WORKDIR /sdk
COPY impl/sdk/ts/package.json impl/sdk/ts/package-lock.json ./
# --ignore-scripts: `prepare` compiles the SDK, which tsx does not need.
RUN npm ci --ignore-scripts
COPY impl/sdk/ts/src ./src
COPY impl/sdk/ts/examples/sandbox ./examples/sandbox
RUN cd examples/sandbox && npm install --no-audit --no-fund

# rn8 first: it reads the task frame, starts the worker with RESONATE_URL on
# its relay, and pushes it the task.
ENTRYPOINT ["rn8", "--", "/sdk/node_modules/.bin/tsx", "/sdk/examples/sandbox/guest.ts"]
