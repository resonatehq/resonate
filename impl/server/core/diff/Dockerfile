FROM rust:1.94-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN cargo test --test differential --no-run

CMD ["cargo", "test", "--test", "differential", "--", "--nocapture"]
