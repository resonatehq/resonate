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

## Distributed Async Await

Resonate's Distributed Async Await is a new programming model that simplifies coding for the cloud. It ensures code completion even if hardware or software failures occur during execution. The programming model does this with just functions and promises, making it trivial to build `coordinated` and `reliable` distributed applications.

## Why Resonate?

- **Cloud Computing Made Dead Simple**: Resonate offers a dead simple programming model that simplifies coding for the cloud using an intuitive paradigm you already know — async await.

- **Single Binary**: Resonate simplifies your deployment and operations with a single binary.

- **Incremental Adoption and No Vendor Lock-In**: Resonate was designed to allow for incremental adoption without vendor lock-in ever.

- **Built on an Open Standard**: Resonate's programming model is built on top of [durable promises](https://github.com/resonatehq/durable-promise-specification), an open standard with an intentionally minimal API surface area.

## Getting Started

1. [Start building in just 30 seconds](https://docs.resonatehq.io/getting-started/quickstart) with our quickstart guide!
2. [Explore in-depth code examples](https://github.com/resonatehq/quickstart-ts/tree/main) and get your hands dirty with our comprehensive repository.
3. [Dive into the TypeScript SDK docs](https://docs.resonatehq.io/sdks/typescript) and learn how to leverage its full potential with our detailed documentation.
4. [Explore the API reference](https://resonatehq.github.io/resonate-sdk-ts/index.html) of Resonate's TypeScript SDK.
5. [Grasp the 4 core concepts](https://docs.resonatehq.io/getting-started/concepts) of distributed async/await applications by delving into our concepts page.
   
## Core features

The basic features Resonate offers to simplify the reliability and coordination of distributed processes are the following:

- **Retries**: If a process fails while executing a durable promise due to a transient issue, such as network connectivity problems, it can be transparently retried, minimizing the impact of temporary failures.

- **Recoverability**: If a process crashes while executing a durable promise, it can recover and continue from where it left off, ensuring your application remains resilient.

- **Schedules**: Durable promises can be used to schedule statefule reminders using a simple HTTP/gRPC call.

- **Task Framework**: Durable promises allow you to fan out tasks across multiple processes or machines, enabling parallel execution and load balancing, making your application more scalable.

- **Notifications**: When a durable promise is created or completed, it can trigger notifications to other processes or services that are interested in the result, enabling efficient communication and coordination.

- **Human in the Loop**: Durable promises can seamlessly integrate human input into your automated workflows, allowing for manual intervention or approval steps when needed.

## Contributing

See our [contribution guidelines](CONTRIBUTING.md).

## License

The Resonate TypeScript SDK is available under the [Apache 2.0 License](LICENSE).
