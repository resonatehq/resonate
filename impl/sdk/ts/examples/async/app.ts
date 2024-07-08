import { Resonate, Context, exponential } from "@resonatehq/sdk";

const resonate = new Resonate({
  timeout: 1000,
  retry: exponential(
    100,      // initial delay (in ms)
    2,        // backoff factor
    Infinity, // max attempts
    60000,    // max delay (in ms, 1 minute)
  ),
});

resonate.register("app", (ctx: Context) => {
  return "Hello World";
});

async function main() {
  await resonate.run("app", "app.1");
}

main();
