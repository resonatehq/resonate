> Resonate is in the **Design Phase**
> 
> Our code base is constantly evolving as we are exploring Resonate's programming model. If you are passionate about a dead simple developer experience, join us on this journey of discovery and share your thoughts.
>
> [Join our slack](https://resonatehqcommunity.slack.com)

<br /><br />
<p align="center">
   <img height="170"src="https://raw.githubusercontent.com/resonatehq/resonate/main/docs/img/echo.png">
</p>

<h1 align="center">Resonate TypeScript SDK</h1>

<div align="center">

[![cicd](https://github.com/resonatehq/resonate-sdk-ts/actions/workflows/cicd.yaml/badge.svg)](https://github.com/resonatehq/resonate-sdk-ts/actions/workflows/cicd.yaml)
[![codecov](https://codecov.io/gh/resonatehq/resonate-sdk-ts/branch/main/graph/badge.svg)](https://codecov.io/gh/resonatehq/resonate-sdk-ts)
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

An SDK for writing simple and elegant distributed async await applications.

## Why Resonate?
Resonate offers a programming model that allows you to build distributed applications using an intuitive paradigm you already know — async await.

## What is Distributed Async Await?

Distributed Async Await extends the async await programming model beyond the boundaries of a single process and makes distributed computing a first-class citizen.

## Features

Available now:
- retries
- recovery
- schedules
- tracing
- logging
- local promise store
- remote promise store

Coming soon:
- rate limiting
- metrics

Let us know [features](https://github.com/resonatehq/resonate-sdk-ts/issues) you would like Resonate to support.

## Install
```console
npm install -g ts-node
npm install @resonatehq/sdk
npm install express @types/express
```

## What's New

Schedules! You can now invoke a function on a recurring schedule. Please note, this feature is experimental and bugs are expected. If encountered please [let us know](https://github.com/resonatehq/resonate-sdk-ts/issues/new).

```ts
import { Resonate, Context } from "@resonatehq/sdk";

const resonate = new Resonate();

resonate.schedule("everyMinute", "* * * * *", (ctx: Context) => {
  console.log("every minute", Date.now());
});

resonate.schedule("everyHour", "0 * * * *", (ctx: Context) => {
  console.log("every hour", Date.now());
});

resonate.start();
```

## Getting Started
```ts
import { Resonate, Context } from "@resonatehq/sdk";
import express, { Request, Response } from "express";

type User = {
  id: number;
};

type Song = {
  id: number;
  price: number;
};

type Status = {
  charged: boolean;
  granted: boolean;
};

async function purchase(ctx: Context, user: User, song: Song): Promise<Status> {
  const charged = await ctx.run(charge, user, song);
  const granted = await ctx.run(access, user, song);

  return { charged, granted };
}

async function charge(ctx: Context, user: User, song: Song): Promise<boolean> {
  console.log(`Charged user:${user.id} $${song.price}.`);
  return true;
}

async function access(ctx: Context, user: User, song: Song): Promise<boolean> {
  console.log(`Granted user:${user.id} access to song:${song.id}.`);
  return true;
}

// Initialize Resonate app
const resonate = new Resonate();
resonate.register("purchase", purchase);

// Initialize Express app
const app = express();
app.use(express.json())

app.post("/purchase", async (req: Request, res: Response) => {
  const user = { id: req.body?.user ?? 1 };
  const song = { id: req.body?.song ?? 1, price: 1.99 };

  // id uniquely identifies the purchase
  const id = `purchase-${user.id}-${song.id}`;

  try {
    res.send(await resonate.run("purchase", id, user, song));
  } catch (err) {
    res.status(500).send("Could not purchase song");
  }
});

app.listen(3000, () => {
  console.log("Listening on port 3000");
});
```

Start the server.
```console
ts-node app.ts
```

And call the endpoint providing a user and song id.
```console
curl \
  -X POST \
  -H 'Content-Type: application/json' \
  -d '{"user": 1, "song": 1}' \
  http://localhost:3000/purchase
```

See our [docs](https://docs.resonatehq.io) for more detailed information.

## Development
```console
npm install
npm run lint
npm test
```

## Contributing
See our [contribution guidelines](CONTRIBUTING.md).

## License
The Resonate TypeScript SDK is available under the [Apache 2.0 License](LICENSE).
