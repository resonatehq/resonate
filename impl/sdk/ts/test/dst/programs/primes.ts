import { Context } from "../../../lib/resonate";

// prime number generator
export async function primes(ctx: Context) {
  for (let n = 0; n < 10; n++) {
    await ctx.run(prime, n);
  }
}

function prime(ctx: Context, n: number) {
  for (let i = 2; i < n; i++) {
    if (n % i === 0) {
      return false;
    }
  }

  return true;
}
