// pipeline shows a multi-stage DAG-shaped durable workflow:
//
//   download → parse → ┬─ transformA ─┐
//                      └─ transformB ─┴─ merge → emit
//
// transformA and transformB run in parallel (both rpc calls are started before
// either is awaited); merge depends on both and synchronizes them with await.
// Every stage is a registered function backed by a durable promise, so a crash
// mid-pipeline picks up at the first unsettled stage without re-doing completed
// work.
//
// Run a Resonate server on localhost:8001 first, then:
//
//   npx tsx examples/async/pipeline.ts

import { type Context, type Info, Resonate } from "../../src/async/index.js";

// merge is the one genuine join point that carries two distinct values; the
// other stages just pass a string / string[] / number, so they need no types.
interface Merged {
  wordCount: number;
  upper: string;
}

// ── Stage functions ─────────────────────────────────────────────────────
//
// Every stage is a leaf: it does its work and returns without spawning
// children, so each takes `Info` rather than the full `Context`. Only the
// runPipeline orchestrator needs the context to fan out via ctx.rpc.

async function download(_info: Info, url: string): Promise<string> {
  const body = `the quick brown fox jumps over ${url}`;
  console.log(`  [download] ${url} -> ${body.length} bytes`);
  return body;
}

async function parse(_info: Info, body: string): Promise<string[]> {
  const words = body.split(/\s+/).filter(Boolean);
  console.log(`  [parse] ${words.length} words`);
  return words;
}

async function transformA(_info: Info, words: string[]): Promise<number> {
  console.log("  [transformA] counting words");
  return words.length;
}

async function transformB(_info: Info, words: string[]): Promise<string> {
  const upper = words.join(" ").toUpperCase();
  console.log(`  [transformB] uppercased ${upper.length} chars`);
  return upper;
}

async function merge(_info: Info, wordCount: number, upper: string): Promise<Merged> {
  console.log("  [merge] combining transforms");
  return { wordCount, upper };
}

async function emit(_info: Info, m: Merged): Promise<string> {
  console.log(`  [emit] words=${m.wordCount} upper=${JSON.stringify(m.upper)}`);
  return "ok";
}

// ── Pipeline orchestrator ───────────────────────────────────────────────

async function runPipeline(ctx: Context, url: string): Promise<string> {
  const body = await ctx.rpc<string>("download", url);
  const words = await ctx.rpc<string[]>("parse", body);

  // fan out: transformA and transformB start in parallel...
  const fA = ctx.rpc<number>("transformA", words);
  const fB = ctx.rpc<string>("transformB", words);

  // ...fan in: await both
  const wordCount = await fA;
  const upper = await fB;

  const merged = await ctx.rpc<Merged>("merge", wordCount, upper);
  return ctx.rpc<string>("emit", merged);
}

// ── main ────────────────────────────────────────────────────────────────

const resonate = new Resonate({ url: "http://localhost:8001" });

const runFn = resonate.register("runPipeline", runPipeline);
resonate.register("download", download);
resonate.register("parse", parse);
resonate.register("transformA", transformA);
resonate.register("transformB", transformB);
resonate.register("merge", merge);
resonate.register("emit", emit);

const id = `pipeline-${Date.now()}`;
console.log(`[runPipeline] starting workflow id=${id}`);

const out = await (await runFn.run(id, "example.com/doc")).result();
console.log(`[runPipeline] OK: sent=${out}`);

await resonate.stop();
