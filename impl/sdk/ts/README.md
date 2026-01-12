![resonate component banner](./assets/resonate-banner.png)

# Resonate TypeScript SDK

[![cicd](https://github.com/resonatehq/resonate-sdk-ts/actions/workflows/cicd.yaml/badge.svg)](https://github.com/resonatehq/resonate-sdk-ts/actions/workflows/cicd.yaml)
[![codecov](https://codecov.io/gh/resonatehq/resonate-sdk-ts/branch/main/graph/badge.svg)](https://codecov.io/gh/resonatehq/resonate-sdk-ts)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## About this component

The Resonate TypeScript SDK enables developers to build reliable and scalable cloud applications across a wide variety of use cases.

- [How to contribute to this SDK](./CONTRIBUTING.md)
- [Evaluate Resonate for your next project](https://docs.resonatehq.io/evaluate/)
- [Example application library](https://github.com/resonatehq-examples)
- [Join the Discord](https://resonatehq.io/discord)
- [Subscribe to the Blog](https://journal.resonatehq.io/subscribe)
- [Follow on Twitter](https://twitter.com/resonatehqio)
- [Follow on LinkedIn](https://www.linkedin.com/company/resonatehqio)
- [Subscribe on YouTube](https://www.youtube.com/@resonatehqio)

## Quickstart

1. Install the Resonate Server & CLI

```shell
brew install resonatehq/tap/resonate
```

2. Install the Resonate SDK

```shell
npm install @resonatehq/sdk
```

3. Write your first Resonate Function

A countdown as a loop. Simple, but the function can run for minutes, hours, or days, despite restarts.

```typescript
import { Resonate, type Context } from "@resonatehq/sdk";

function* countdown(context: Context, count: number, delay: number) {
  for (let i = count; i > 0; i--) {
    // Run a function, persist its result
    yield* context.run((context: Context) => console.log(`Countdown: ${i}`));
    // Sleep
    yield* context.sleep(delay * 1000);
  }
  console.log("Done!");
}
// Instantiate Resonate
const resonate = new Resonate({ url: "http://localhost:8001" });
// Register the function
resonate.register(countdown);
```

[Clone a working example repo](https://github.com/resonatehq-examples/example-quickstart-ts)

4. Start the server

```shell
resonate dev
```

5. Start the worker

```shell
npx ts-node countdown.ts
```

6. Run the function

Run the function with execution ID `countdown.1`:

```shell
resonate invoke countdown.1 --func countdown --arg 5 --arg 60
```

**Result**

You will see the countdown in the terminal

```shell
npx ts-node countdown.ts
Countdown: 5
Countdown: 4
Countdown: 3
Countdown: 2
Countdown: 1
Done!
```

**What to try**

After starting the function, inspect the current state of the execution using the `resonate tree` command. The tree command visualizes the call graph of the function execution as a graph of durable promises.

```shell
resonate tree countdown.1
```

Now try killing the worker mid-countdown and restarting. **The countdown picks up right where it left off without missing a beat.**
