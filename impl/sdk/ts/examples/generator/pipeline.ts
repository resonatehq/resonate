// pipeline shows a multi-stage DAG-shaped durable workflow:
//
//   download → parse → ┬─ transformA ─┐
//                      └─ transformB ─┴─ merge → emit
//
// transformA and transformB run in parallel (both started via beginRpc before
// either is awaited); merge depends on both and synchronizes them. Every stage
// is a registered function backed by a durable promise, so a crash mid-pipeline
// picks up at the first unsettled stage without re-doing completed work.
//
// Start a Resonate server on localhost:8001 first (`resonate dev`), then:
//
//   npx tsx examples/generator/pipeline.ts

import { type Context, Resonate } from "../../src/index.js";

interface Merged {
  wordCount: number;
  upper: string;
}

// -- Stage functions (leaves: each does its work and returns) -------------

async function download(_ctx: Context, url: string): Promise<string> {
  const body = `the quick brown fox jumps over ${url}`;
  console.log(`  [download] ${url} -> ${body.length} bytes`);
  return body;
}

async function parse(_ctx: Context, body: string): Promise<string[]> {
  const words = body.split(/\s+/).filter(Boolean);
  console.log(`  [parse] ${words.length} words`);
  return words;
}

async function transformA(_ctx: Context, words: string[]): Promise<number> {
  console.log("  [transformA] counting words");
  return words.length;
}

async function transformB(_ctx: Context, words: string[]): Promise<string> {
  const upper = words.join(" ").toUpperCase();
  console.log(`  [transformB] uppercased ${upper.length} chars`);
  return upper;
}

async function merge(_ctx: Context, wordCount: number, upper: string): Promise<Merged> {
  console.log("  [merge] combining transforms");
  return { wordCount, upper };
}

async function emit(_ctx: Context, m: Merged): Promise<string> {
  console.log(`  [emit] words=${m.wordCount} upper=${JSON.stringify(m.upper)}`);
  return "ok";
}

// -- Pipeline orchestrator ------------------------------------------------

function* runPipeline(ctx: Context, url: string): Generator<any, string, any> {
  const body: string = yield* ctx.rpc<string>("download", url);
  const words: string[] = yield* ctx.rpc<string[]>("parse", body);

  // fan out: transformA and transformB start in parallel...
  const fA = yield* ctx.beginRpc<number>("transformA", words);
  const fB = yield* ctx.beginRpc<string>("transformB", words);

  // ...fan in: await both
  const wordCount: number = yield* fA;
  const upper: string = yield* fB;

  const merged: Merged = yield* ctx.rpc<Merged>("merge", wordCount, upper);
  return yield* ctx.rpc<string>("emit", merged);
}

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
resonate.register("runPipeline", runPipeline);
resonate.register("download", download);
resonate.register("parse", parse);
resonate.register("transformA", transformA);
resonate.register("transformB", transformB);
resonate.register("merge", merge);
resonate.register("emit", emit);

try {
  const id = `pipeline-${Date.now()}`;
  console.log(`[runPipeline] starting workflow id=${id}`);
  const out = await resonate.run(id, runPipeline, "example.com/doc");
  console.log(`[runPipeline] OK: sent=${out}`);
} finally {
  await resonate.stop();
}
