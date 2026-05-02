# Stage 1: Build the SPA
FROM node:22-bookworm-slim AS spa-builder

WORKDIR /app/web

COPY web/package.json web/package-lock.json* ./
RUN npm ci

COPY web/ .
RUN npm run build

# Stage 2: Build the Rust binary
FROM rust:slim-bookworm AS rust-builder

RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
COPY --from=spa-builder /app/web/dist/ ./web/dist/

RUN cargo build --release

# Stage 3: Runtime image
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates wget && rm -rf /var/lib/apt/lists/*

COPY --from=rust-builder /app/target/release/resonate /usr/local/bin/resonate

EXPOSE 8001

ENTRYPOINT ["resonate"]
CMD ["serve"]
