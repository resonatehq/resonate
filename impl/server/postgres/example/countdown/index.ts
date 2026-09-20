// An Edge Function that takes time.
import { type Context, Resonate } from "jsr:@resonatehq/supabase@0.4.1";
import { createClient } from "jsr:@supabase/supabase-js@^2";

const resonate = new Resonate();

const supabase = createClient(
  Deno.env.get("SUPABASE_URL")!,
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
);

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
      await ctx.run((ctx: Context) =>
        broadcast(ctx.originId, `countdown: ${i}`),
      );
      await ctx.sleep(60 * 1000);
    }
    await ctx.run((ctx: Context) => broadcast(ctx.originId, "liftoff 🚀"));
  },
);

resonate.httpHandler();
