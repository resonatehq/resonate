<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../../assets/resonate-banner.png">
  <img alt="Resonate" src="../../assets/resonate-banner-light.png">
</picture>

# Research on Supabase

A durable research agent on resonate-pg using the [Anthropic SDK](https://docs.claude.com/en/api/overview). It turns a question into a search plan, then fans the searches out — each one runs on its own Edge Function invocation, all in parallel — and writes a cited report from what they find. While the searches run, the workflow that spawned them suspends: it holds no process, just a row in Postgres waiting on its children. When they all land it resumes in a fresh invocation to write the report. Every step is checkpointed, so the run survives crashes, redeploys, and the Edge Function wall-clock limit.

```ts
resonate.register(
  "research",
  async function research(ctx: Context, question: string) {
    // 1 · plan · one LLM call turns the question into a strategy
    const plan = await ctx.run(() => planSearches(question));

    // 2 · search · each runs on its own invocation; the parent suspends here.
    //     a failed or slow search is skipped, not fatal (partial results).
    const findings = await Promise.all(
      plan.searches.map(async (s) => {
        try {
          return await ctx.rpc("search", s.query);
        } catch {
          return { query: s.query, text: "" };
        }
      }),
    );

    // 3 · report · write the cited report — one LLM call over the findings
    return await ctx.run(() => report(question, findings));
  },
);
```

Full function: [`index.ts`](index.ts). Each `search` is one hosted web search with a per-call timeout, so a slow one fails fast and is skipped rather than stranding the run; the example keeps it lean at `max_uses: 1`.

## 1. Create the project

```bash
supabase projects create resonate-research
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
mkdir -p supabase/functions/research && cp example/research/index.ts supabase/functions/research/index.ts
supabase functions deploy research --no-verify-jwt
```

## 4. Add your Anthropic key

The function calls Claude for the plan, the searches, and the report, so it needs an API key:

```bash
supabase secrets set ANTHROPIC_API_KEY=sk-ant-...
```

## 5. Start it

```bash
supabase db query --linked "
  select resonate.invoke('research-1', 'research', '[\"What is Resonate Durable Execution?\"]'::jsonb,
    'https://<project-ref>.functions.supabase.co/research');"
```

## 6. Read the report

The run resolves in a couple of minutes. Its value is the report — read it from the resolved promise (the query returns nothing until then):

```bash
supabase db query --linked "
  select (convert_from(decode(value_data, 'base64'), 'utf8'))::jsonb #>> '{}' as report
  from resonate.promises where id = 'research-1' and state = 'resolved';"
```

## 7. Cleanup

Delete the project:

```bash
supabase projects delete <project-ref>
```
