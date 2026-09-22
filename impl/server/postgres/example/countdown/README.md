<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../../assets/resonate-banner.png">
  <img alt="Resonate" src="../../assets/resonate-banner-light.png">
</picture>

# Countdown on Supabase

A durable countdown on resonate-pg. It counts down from `n`, sleeping a minute between steps — and while it sleeps, **nothing runs anywhere**. Each step is saved to Postgres, which wakes the function for the next one. Make the sleep a day and it's a daily job; the invocations stay milliseconds long either way, and the run survives crashes and redeploys.

```ts
// Broadcast a message on a Supabase Realtime channel (event "tick").
const broadcast = (channel: string, message: string) =>
  supabase.channel(channel).send({
    type: "broadcast",
    event: "tick",
    payload: { message },
  });

resonate.register(
  "countdown",
  async function countdown(ctx: Context, n: number) {
    for (let i = n; i > 0; i--) {
      await ctx.run((ctx: Context) => broadcast(ctx.originId, `countdown: ${i}`));
      await ctx.sleep(60 * 1000);
    }
    await ctx.run((ctx: Context) => broadcast(ctx.originId, "liftoff 🚀"));
  },
);
```

Full function: [`index.ts`](index.ts).

## 1. Create the project

```bash
supabase projects create resonate-countdown
```

The command prompts for your org, region, and a database password, then prints the project **ref** used below.

## 2. Install the server

Link the project once, then apply the extensions and `resonate.sql` — every step runs over the Management API, no connection string needed:

```bash
supabase link --project-ref <project-ref>
supabase db query --linked "create extension if not exists pg_cron; create extension if not exists pg_net;"
supabase db query --linked -f resonate.sql
```

## 3. Deploy the function

```bash
mkdir -p supabase/functions/countdown && cp example/countdown/index.ts supabase/functions/countdown/index.ts
supabase functions deploy countdown --no-verify-jwt
```

## 4. Watch it

Navigate to the [supabase Dashboard](https://supabase.com/dashboard) → *resonate-countdown* → Realtime → Inspector: Join a channel: Name of channel: *countdown-1*.

## 5. Start it

```bash
supabase db query --linked "
  select resonate.invoke('countdown-1', 'countdown', '[3]'::jsonb,
    'https://<project-ref>.functions.supabase.co/countdown');"
```

## 6. Cleanup

Delete the project:

```bash
supabase projects delete <project-ref>
```
