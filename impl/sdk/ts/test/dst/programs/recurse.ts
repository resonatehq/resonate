import { Context } from "../../../lib/resonate";

export async function recurse(ctx: Context, n: number = 3) {
  for (let i = 0; i < n; i++) {
    await ctx.run(recurse, n - 1);
  }
}
