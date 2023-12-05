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

## Why Resonate?
Resonate offers a programming model that allows you to build distributed applications using an intuitive paradigm you already know — async await.

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
import express from "express";
import { Resonate, Context } from "@resonatehq/sdk";

/* Your async code using Resonate's Context */
type User = {
  id: number;
  name: string;
};

type Song = {
  id: number;
  title: string;
};

// Purchase song event handler
async function purchase(ctx: Context, user: User, song: Song): Promise<{ charge: any; access: any }> {
  const charge = await ctx.run(chargeCreditCard, user, song);
  const access = await ctx.run(unlockUserAccess, user, song);
  return { charge, access };
}

async function chargeCreditCard(ctx: Context, user: User, song: Song): Promise<any> {
  console.log("Charging credit card...");
  return { status: "charged" };
}

async function unlockUserAccess(ctx: Context, user: User, song: Song): Promise<any> {
  console.log("Unlocking user access...");
  return { status: "unlocked" };
}

/* Express Application w/ Resonate Event Handler */

// Create Resonate instance
const resonate = new Resonate();

// Register purchase handler
resonate.register("durablePurchase", purchase);

// Initialize Express app with purchase route
const app = express();

app.post("/purchase", async (req, res) => {
  
  // Dummy user and song data
  const user = { id: 1, name: "John" };
  const song = { id: 1, title: "Song 1" };

  // Generate unique ID for purchase execution. This is used to track the execution. 
  // Typically, this would be an external ID from your incoming request.
  const purchaseId = `purchase-${user.id}-${song.id}`

  // Execute durable purchase
  try {
    const result = await resonate.run("durablePurchase", purchaseId, user, song);
    res.send(result);
  } catch (err) {
    res.status(500).send("Unable to purchase song");
  }
});

app.listen(3000, () => {
  console.log("App listening on port 3000");
});

```

Once the server is running, invoke the purchase endpoint.
```bash
curl -X POST localhost:3000/purchase
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

## License
The Resonate TypeScript SDK is available under the [Apache 2.0 License](LICENSE).
