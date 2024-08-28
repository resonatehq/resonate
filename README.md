<p align="center">
    <img height="170"src="./docs/img/echo.png">
</p>

<h1 align="center">Resonate</h1>

<div align="center">

[![ci](https://github.com/resonatehq/resonate/actions/workflows/cicd.yaml/badge.svg)](https://github.com/resonatehq/resonate/actions/workflows/cicd.yaml)
[![dst](https://github.com/resonatehq/resonate/actions/workflows/dst.yaml/badge.svg)](https://github.com/resonatehq/resonate/actions/workflows/dst.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/resonatehq/resonate)](https://goreportcard.com/report/github.com/resonatehq/resonate)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

</div>

<div align="center">
<a href="https://docs.resonatehq.io">Docs</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://twitter.com/resonatehqio">Twitter</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://resonatehq.io/discord">Discord</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/resonatehq/resonate/issues">Issues</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/resonatehq/resonate/issues/131">Roadmap</a>
  <br />
</div>

## Distributed Async Await

Resonate's Distributed Async Await is a new programming model that simplifies coding for the cloud. It ensures code completion even if hardware or software failures occur during execution. The programming model does this with just functions and promises, making it trivial to build `coordinated` and `reliable` distributed applications.

## Why Resonate?

- **Cloud Computing Made Dead Simple**: Resonate simplifies coding for the cloud using an intuitive paradigm you already know — async await.

- **Single Binary**: Resonate simplifies your deployment and operations with a single binary.

- **Incremental Adoption and No Vendor Lock-In**: Resonate was designed to allow for incremental adoption without vendor lock-in ever.

- **Built on an Open Standard**: Resonate's programming model is built on top of [durable promises](https://github.com/resonatehq/durable-promise-specification), an open standard with an intentionally minimal API surface area.

## Getting Started

1. [Start building in just 30 seconds](https://docs.resonatehq.io/getting-started/quickstart) with our quickstart guide!
2. [Grasp the 4 core concepts](https://docs.resonatehq.io/getting-started/concepts) of distributed async/await applications by delving into our concepts page.
3. [Dive into the Resonate Server docs](https://docs.resonatehq.io/resonate/overview) and learn how to configure, deploy, and more.

## SDKs

Resonate provides client SDKs for different programming languages to easily interact with the Resonate server and write elegant distributed async await applications. More are coming soon!

| Type   | Language                                                                                                            | Source Code                                   | Package                                              |
| ------ | ------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- | ---------------------------------------------------- |
| TS SDK | <img alt="TS SDK" src="https://upload.wikimedia.org/wikipedia/commons/4/4c/Typescript_logo_2020.svg" width="50px"/> | https://github.com/resonatehq/resonate-sdk-ts | [NPM](https://www.npmjs.com/package/@resonatehq/sdk) |

## Core Features

The basic features Resonate offers to simplify the reliability and coordination of distributed processes are the following:

- **Retries**: If a process fails while executing a durable promise due to a transient issue, such as network connectivity problems, it can be transparently retried, minimizing the impact of temporary failures.

- **Recoverability**: If a process crashes while executing a durable promise, it can recover and continue from where it left off, ensuring your application remains resilient.

- **Schedules**: Durable promises can be used to schedule stateful reminders using a simple HTTP/gRPC call.

- **Task Framework**: Durable promises allow you to fan out tasks across multiple processes or machines, enabling parallel execution and load balancing, making your application more scalable.

- **Notifications**: When a durable promise is created or completed, it can trigger notifications to other processes or services that are interested in the result, enabling efficient communication and coordination.

- **Human in the Loop**: Durable promises can seamlessly integrate human input into your automated workflows, allowing for manual intervention or approval steps when needed.

## Contributing

See our [contribution guidelines](CONTRIBUTING.md).

## License

The Resonate Server is available under the [Apache 2.0 License](LICENSE).
