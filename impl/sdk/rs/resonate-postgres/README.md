# resonate-sdk-postgres

Postgres network for the [Resonate Rust SDK](https://github.com/resonatehq/resonate-sdk-rs): durable execution on nothing but Postgres. The Resonate protocol runs as stored procedures in a `resonate` schema — see [resonate-pg](https://github.com/resonatehq/resonate-pg). No Resonate server required.

## Setup

Apply the schema to your database (Postgres 16+):

```bash
psql -d yourdb -f resonate.sql   # from github.com/resonatehq/resonate-pg
```

Add the dependencies:

```toml
[dependencies]
resonate-sdk = "0.5"
resonate-sdk-postgres = "0.5"
```

## Usage

```rust
use std::sync::Arc;
use resonate_sdk::prelude::*;
use resonate_sdk_postgres::PostgresNetwork;

let network = PostgresNetwork::new("postgres://user:pass@localhost:5432/db");

let resonate = Resonate::new(ResonateConfig {
    network: Some(Arc::new(network)),
    ..Default::default()
});
```

The worker id, routing group, and fallback poll interval can be set with the builder:

```rust
let network = PostgresNetwork::builder("postgres://user:pass@localhost:5432/db")
    .pid("worker-1")                    // defaults to a generated id
    .group("workers")                   // defaults to "default"
    .tick(Duration::from_millis(500))   // defaults to 250ms
    .build();
```

TLS is supported out of the box; connection strings with `sslmode=require` (e.g. Supabase) work as-is.
