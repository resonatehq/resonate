// A durable research agent that streams live to a Supabase Realtime channel.
import { type Context, Resonate } from "jsr:@resonatehq/supabase@0.4.1";
import { createClient } from "npm:@supabase/supabase-js@^2.110";
import Anthropic from "npm:@anthropic-ai/sdk@^0.111";

const resonate = new Resonate();

const supabase = createClient(
  Deno.env.get("SUPABASE_URL")!,
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
);

const claude = new Anthropic({ apiKey: Deno.env.get("ANTHROPIC_API_KEY")! });

type Search = { query: string; reason: string };
type Result = { query: string; text: string };

resonate.register(
  "research",
  async function research(context: Context, question: string) {
    const searches = await context.run(plan, question);

    const results = await Promise.all(
      searches.map(async (s) => {
        try {
          return await context.rpc<Result>("search", s.query);
        } catch {
          return { query: s.query, text: "" };
        }
      }),
    );

    return await context.run(report, question, results);
  },
);

resonate.register(
  "search",
  async function search(context: Context, query: string): Promise<Result> {
    const text = await streamClaude(
      streamer(context.originId, context.id, query),
      {
        model: "claude-sonnet-5",
        max_tokens: 2048,
        tools: [
          {
            type: "web_search_20260209",
            name: "web_search",
            max_uses: 3,
            allowed_callers: ["direct"],
          },
        ],
        messages: [
          {
            role: "user",
            content: `Research and summarize, with citations: ${query}`,
          },
        ],
      },
      { maxRetries: 0 },
    );
    return { query, text };
  },
);

async function plan(_context: Context, question: string): Promise<Search[]> {
  const m = await claude.messages.create({
    model: "claude-opus-4-8",
    max_tokens: 1024,
    tool_choice: { type: "tool", name: "plan" },
    tools: [
      {
        name: "plan",
        description: "A web-search strategy for answering the question.",
        strict: true,
        input_schema: {
          type: "object",
          additionalProperties: false,
          properties: {
            searches: {
              type: "array",
              items: {
                type: "object",
                additionalProperties: false,
                properties: {
                  query: { type: "string", description: "The search query." },
                  reason: {
                    type: "string",
                    description: "Why this search advances the question.",
                  },
                },
                required: ["query", "reason"],
              },
            },
          },
          required: ["searches"],
        },
      },
    ],
    messages: [
      {
        role: "user",
        content: `Plan the web searches needed to answer: ${question}`,
      },
    ],
  });
  const tool = m.content.find(
    (b): b is Anthropic.ToolUseBlock => b.type === "tool_use",
  );
  return (tool!.input as { searches: Search[] }).searches;
}

async function report(
  context: Context,
  question: string,
  results: Result[],
): Promise<string> {
  const done = results.filter((r) => r.text);
  const body = done.map((r) => `## ${r.query}\n\n${r.text}`).join("\n\n");
  return await streamClaude(streamer(context.originId, context.id, "Report"), {
    model: "claude-opus-4-8",
    max_tokens: 8000,
    thinking: { type: "adaptive" },
    messages: [
      {
        role: "user",
        content: `Question: ${question}\n\n` +
          `Results from ${done.length} web searches:\n\n${body}\n\n` +
          `Write a well-structured, cited report that answers the question.`,
      },
    ],
  });
}

async function streamClaude(
  s: ReturnType<typeof streamer>,
  params: Parameters<typeof claude.messages.stream>[0],
  opts?: Parameters<typeof claude.messages.stream>[1],
): Promise<string> {
  await s.bos();
  let buf = "";
  const flush = async () => {
    if (buf) {
      const d = buf;
      buf = "";
      await s.chunk(d);
    }
  };
  const textOf = (m: Anthropic.Message) =>
    m.content
      .filter((b): b is Anthropic.TextBlock => b.type === "text")
      .map((b) => b.text)
      .join("\n")
      .trim();
  try {
    const stream = claude.messages.stream(params, opts);
    for await (const ev of stream) {
      if (ev.type === "content_block_delta" && ev.delta.type === "text_delta") {
        buf += ev.delta.text;
        if (buf.length >= 48) await flush();
      }
    }
    await flush();
    const message = await stream.finalMessage();
    // A long-running search turn can stop early with pause_turn — the text so
    // far is a partial answer, not a result. Fail rather than return it as if
    // complete; the caller drops this search, same as an abort.
    if (message.stop_reason === "pause_turn") {
      throw new Error("search paused before completing (pause_turn)");
    }
    return textOf(message);
  } finally {
    await s.eos();
  }
}

function streamer(origin: string, id: string, label: string) {
  const channel = supabase.channel(origin);
  return {
    bos: async () => {
      await channel.httpSend("stream", { id, kind: "bos", label });
    },
    chunk: async (data: string) => {
      await channel.httpSend("stream", { id, kind: "chunk", data });
    },
    eos: async () => {
      await channel.httpSend("stream", { id, kind: "eos" });
      await supabase.removeChannel(channel);
    },
  };
}

resonate.httpHandler();
