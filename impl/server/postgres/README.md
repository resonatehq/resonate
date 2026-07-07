<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./assets/resonate-banner.png">
  <img alt="Resonate" src="./assets/resonate-banner-light.png">
</picture>

# Resonate on Postgres

**Dead simple durable execution.**

Resonate durable execution runs your code as a reliable workflow, checkpointing each step as a durable promise, sleeping for days, surviving crashes and restarts. resonate-pg is one SQL file. No additional servers, queues, or timers — Postgres, with pg_cron, is all three. Full Resonate docs live at [docs.resonatehq.io](https://docs.resonatehq.io).

```ts
resonate.register(
  "countdown",
  async function countdown(ctx: Context, n: number) {
    for (let i = n; i > 0; i--) {
      await ctx.run(() => console.log(`countdown: ${i}`));
      await ctx.sleep(10 * 60 * 1000); // durable: no process waits
    }
    await ctx.run(() => console.log("liftoff 🚀"));
  },
);
``` 

Crash the process, redeploy, or lose the machine mid-run — the workflow resumes on the right number, and nothing runs twice. Each `ctx.run` is checkpointed to Postgres as it completes, so a resumed run replays finished steps from the database instead of re-executing them. And `ctx.sleep` is just a row with a deadline: the invocation returns and nothing runs until it fires, whether that's ten minutes or ten days from now.

> **On Supabase?** [`example/countdown`](example/countdown) goes from an empty project to a running durable workflow in about five minutes.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./assets/quickstart-banner.png">
  <img alt="Quickstart" src="./assets/quickstart-banner-light.png">
</picture>

## Getting started

resonate-pg is a Resonate Server in a single file running on Postgres 16+:

```bash
psql -d yourdb -f resonate.sql
```

**Extensions**

- pg_cron *(required — drives all timers)*
- pg_net or pgsql_http *(optional — either one enables HTTP push delivery)*

## SDK

You write workflows with a Resonate SDK:

- [TypeScript](https://github.com/resonatehq/resonate-sdk-ts)
- [Python](https://github.com/resonatehq/resonate-sdk-py)
- [Go](https://github.com/resonatehq/resonate-sdk-go)
- [Java](https://github.com/resonatehq/resonate-sdk-java)
- [Rust](https://github.com/resonatehq/resonate-sdk-rs)

## Operations

Completed workflows stay in the database. Delete old ones on a schedule:

```sql
-- daily at 03:00: delete workflows finished more than 7 days ago
select cron.schedule('resonate-gc', '0 3 * * *',
  $$select resonate.gc((extract(epoch from now())*1000 - 7*86400000)::bigint)$$);
```

Keep the horizon longer than any window in which you might re-send the same workflow id; ids are idempotent only while the row exists.

## Under the hood

resonate-pg is a faithful implementation of the Resonate protocol: every protocol action is a stored procedure, callable via resonate_rpc:

```sql
SELECT resonate.resonate_rpc('{"kind":"promise.get","head":{},"data":{"id":"invoke:foo"}}');
```

## Community

Questions, ideas, or want to help? Join the [Resonate Discord](https://resonatehq.io/discord), or open an issue or pull request — contributions welcome. resonate-pg is part of [Resonate](https://github.com/resonatehq/resonate).

## License

[Apache 2.0](./LICENSE).
