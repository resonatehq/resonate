> Resonate is in the **Design Phase**
> 
> Our code base is constantly evolving as we are exploring Resonate's programming model. If you are passionate about a dead simple developer experience, join us on this journey of discovery and share your thoughts.
>
> [Join our slack](https://resonatehqcommunity.slack.com)

<p align="center">
   <img height="170"src="https://raw.githubusercontent.com/resonatehq/resonate/main/docs/img/echo.png">
</p>

<h1 align="center">Resonate TypeScript SDK</h1>

<div align="center">

[![ci](https://github.com/resonatehq/resonate-sdk-ts/actions/workflows/ci.yaml/badge.svg)](https://github.com/resonatehq/resonate-sdk-ts/actions/workflows/ci.yaml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

</div>

<div align="center">
  <a href="https://docs.resonatehq.io">Docs</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://twitter.com/resonatehqio">Twitter</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://resonatehqcommunity.slack.com">Slack</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/resonatehq/resonate-sdk-ts/issues">Issues</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/resonatehq/resonate/issues/131">Roadmap</a>
  <br /><br />
</div>

An SDK for writing simple and elegant durable async await applications.

## What is Durable Async Await?

Durable Async Await are Functions and Promises that maintain progress in durable storage.

## Features

Available now:
- durable async await
- volatile async await
- retries
- tracing
- logging

Coming soon:
- rate limiting
- metrics

Let us know [features](https://github.com/resonatehq/resonate-sdk-ts/issues) you would like Resonate to support.

## Install
```bash
npm install https://github.com/resonatehq/resonate-sdk-ts
```

## Getting Started
```typescript
import { Resonate, Context } from 'resonate-sdk-ts';

// Initialize Resonate
const resonate = new Resonate();

// Register a durable function
resonate.register('durableAdd', durableAdd);

// Call a durable function
const r = await resonate.run('durableAdd', 'id', 1, 1);
console.log(r); // 2

async function durableAdd(c: Context, a: number, b: number): number {
   return await c.run(add, a, b);
}

function add(c: Context, a: number, b: number): number {
   return a + b;
}
```

See our [docs](https://docs.resonatehq.io) for more detailed information.

## Development
```bash
npm install
npm run lint
npm test
```

## Contributing
See our [contribution guidelines](CONTRIBUTING.md).
