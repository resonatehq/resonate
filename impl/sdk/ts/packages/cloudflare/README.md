# @resonatehq/cloudflare

`@resonatehq/cloudflare` is the official binding to run [Resonate](https://github.com/resonatehq/resonate) durable execution workers on [Cloudflare Workers](https://workers.cloudflare.com). Write long-running, stateful applications on short-lived, stateless serverless infrastructure.

## Installation

```bash
npm install @resonatehq/cloudflare
```

## How it works

When a Durable Function suspends (e.g. on `yield* context.rpc()` or `context.sleep()`), the Cloudflare Worker **terminates**. When the Durable Promise completes, the Resonate Server resumes the function by invoking the Worker again — no long-running process required.

![Resonate on Serverless](./public/resonate.svg)

## Usage

Register your functions and export the HTTP handler from your Worker entry point:

```ts
import { Resonate } from "@resonatehq/cloudflare";
import type { Context } from "@resonatehq/cloudflare";

const resonate = new Resonate();

resonate.register("countdown", function* countdown(ctx: Context, n: number): Generator {
  if (n <= 0) {
    console.log("done");
    return;
  }
  console.log(n);
  yield* ctx.sleep(1000);
  yield* ctx.rpc(countdown, n - 1);
});

// Export as a Cloudflare Workers fetch handler
export default {
  fetch: resonate.httpHandler(),
};
```

Deploy this as a Cloudflare Worker. The Resonate Server will call your Worker to invoke and resume durable functions.

See the [Cloudflare Workers documentation](https://developers.cloudflare.com/workers/) to learn how to develop and deploy Workers.

## Examples

- [Durable Countdown on Cloudflare Workers](https://github.com/resonatehq-examples/example-countdown-cloudflare-ts)
- [Durable Research Agent on Cloudflare Workers](https://github.com/resonatehq-examples/example-openai-deep-research-agent-cloudflare-ts)

## Documentation

Full documentation: [docs.resonatehq.io](https://docs.resonatehq.io)
