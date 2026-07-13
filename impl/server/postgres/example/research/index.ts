// A durable research agent: plan → search → report.
import { type Context, Resonate } from "jsr:@resonatehq/supabase@0.4.1";
import Anthropic from "npm:@anthropic-ai/sdk@^0.111";

const resonate = new Resonate();

const claude = new Anthropic({ apiKey: Deno.env.get("ANTHROPIC_API_KEY")! });

const textOf = (m: Anthropic.Message) =>
  m.content
    .filter((b): b is Anthropic.TextBlock => b.type === "text")
    .map((b) => b.text)
    .join("\n")
    .trim();

type Plan = { searches: { query: string; reason: string }[] };
type Finding = { query: string; text: string };

resonate.register(
  "research",
  async function research(ctx: Context, question: string) {
    // 1 · PLAN. One LLM call turns the question into a search strategy.
    const plan = (await ctx.run(() => planSearches(question))) as Plan;

    // 2 · SEARCH. Dispatch each search as a remote call, then await together.
    // Each `ctx.rpc` starts a `search` in its own invocation; the parent
    // suspends here until all of them resolve.
    const findings = (await Promise.all(
      plan.searches.map(async (s) => {
        try {
          return await ctx.rpc("search", s.query);
        } catch {
          return { query: s.query, text: "" };
        }
      }),
    )) as Finding[];

    // 3 · REPORT. One LLM call synthesizes the cited report.
    return (await ctx.run(() => report(question, findings))) as string;
  },
);

resonate.register(
  "search",
  async function search(ctx: Context, query: string): Promise<Finding> {
    const text = (await ctx.run(async () => {
      const m = await claude.messages.create(
        {
          model: "claude-sonnet-5",
          max_tokens: 2048,
          tools: [
            { type: "web_search_20260209", name: "web_search", max_uses: 1 },
          ],
          messages: [
            {
              role: "user",
              content: `Research and summarize, with citations: ${query}`,
            },
          ],
        },
        { timeout: 140_000, maxRetries: 0 },
      );
      return textOf(m);
    })) as string;
    return { query, text };
  },
);

resonate.httpHandler();

// --- the intelligence: discrete structured Claude calls ---------------------

// A forced, strict tool call guarantees a valid { searches: [...] } object.
async function planSearches(question: string): Promise<Plan> {
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
  return tool!.input as Plan;
}

async function report(
  question: string,
  findings: Finding[],
): Promise<string> {
  const done = findings.filter((f) => f.text);
  const body = done.map((f) => `## ${f.query}\n\n${f.text}`).join("\n\n");
  const m = await claude.messages.create({
    model: "claude-opus-4-8",
    max_tokens: 8000,
    thinking: { type: "adaptive" },
    messages: [
      {
        role: "user",
        content:
          `Question: ${question}\n\n` +
          `Findings from ${done.length} web searches:\n\n${body}\n\n` +
          `Write a well-structured, cited report that answers the question.`,
      },
    ],
  });
  return textOf(m);
}
