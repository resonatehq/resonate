# @resonatehq/aws

`@resonatehq/aws` is the official binding to run [Resonate](https://github.com/resonatehq/resonate) durable execution workers on [AWS Lambda](https://aws.amazon.com/pm/lambda). Write long-running, stateful applications on short-lived, stateless serverless infrastructure.

## Installation

```bash
npm install @resonatehq/aws
```

## How it works

When a Durable Function suspends (e.g. on `yield* context.rpc()` or `context.sleep()`), the Lambda function **terminates**. When the Durable Promise completes, the Resonate Server resumes the function by invoking Lambda again — no long-running process required.

![Resonate on Serverless](./public/resonate.svg)

## Usage

Register your functions and export the HTTP handler from your Lambda entry point:

```ts
import { Resonate } from "@resonatehq/aws";
import type { Context } from "@resonatehq/aws";

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

// Export as an AWS Lambda Function URL handler
export const handler = resonate.httpHandler();
```

Deploy this as an [AWS Lambda Function URL](https://docs.aws.amazon.com/lambda/latest/dg/lambda-urls.html). The Resonate Server will call your handler to invoke and resume durable functions.

See the [AWS Lambda documentation](https://docs.aws.amazon.com/lambda/latest/dg/welcome.html) to learn how to develop and deploy Lambda functions.

## Examples

- [Durable Countdown on AWS Lambda](https://github.com/resonatehq-examples/example-countdown-ts)
- [Durable Research Agent on AWS Lambda](https://github.com/resonatehq-examples/example-openai-deep-research-agent-ts)

## Documentation

Full documentation: [docs.resonatehq.io](https://docs.resonatehq.io)
