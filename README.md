> Resonate is in the **Design Phase**
>
> Our code base is constantly evolving as we are exploring Resonate's programming model. If you are passionate about a dead simple developer experience, join us on this journey of discovery and share your thoughts.
>
> [Join our slack](https://resonatehqcommunity.slack.com)

<br /><br />

<p align="center">
    <img height="170"src="./docs/img/echo.png">
</p>

<h1 align="center">Resonate</h1>

<div align="center">

[![ci](https://github.com/resonatehq/resonate/actions/workflows/cicd.yaml/badge.svg)](https://github.com/resonatehq/resonate/actions/workflows/cicd.yaml)
[![dst](https://github.com/resonatehq/resonate/actions/workflows/dst.yaml/badge.svg)](https://github.com/resonatehq/resonate/actions/workflows/dst.yaml)
[![codecov](https://codecov.io/gh/resonatehq/resonate/branch/main/graph/badge.svg)](https://codecov.io/gh/resonatehq/resonate)
[![Go Report Card](https://goreportcard.com/badge/github.com/resonatehq/resonate)](https://goreportcard.com/report/github.com/resonatehq/resonate)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

</div>

<div align="center">
<a href="https://docs.resonatehq.io">Docs</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://twitter.com/resonatehqio">Twitter</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://resonatehqcommunity.slack.com">Slack</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/resonatehq/resonate/issues">Issues</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/resonatehq/resonate/issues/131">Roadmap</a>
  <br />
</div>

## Why Resonate?

Resonate offers a programming model that allows you to build distributed applications using an intuitive paradigm you already know — async await.

## What is Distributed Async Await?

Distributed Async Await extends the async await programming model beyond the boundaries of a single process and makes distributed computing a first-class citizen.

## Install

Resonate is currently in active development without a formal release cycle. We welcome early adopters to experiment with the latest build from main as we work towards our first stable release. Your [feedback](https://github.com/resonatehq/resonate/issues/new/choose) is greatly appreciated.

|  OS   | Architecture |                                                                               Link |
| :---: | :----------: | ---------------------------------------------------------------------------------: |
| MacOS |    x86_64    |  [Install](https://storage.googleapis.com/resonate-release/darwin-x86_64/resonate) |
| MacOS |   aarch64    | [Install](https://storage.googleapis.com/resonate-release/darwin-aarch64/resonate) |
| Linux |    x86_64    |   [Install](https://storage.googleapis.com/resonate-release/linux-x86_64/resonate) |
| Linux |   aarch64    |  [Install](https://storage.googleapis.com/resonate-release/linux-aarch64/resonate) |

## Getting Started

Resonate makes it easy to get started creating and interacting with durable promises. Follow these steps to build and run Resonate, then start creating and completing promises.

1. **Build and Run**

   The resonate server supports `http` and `grpc` protocols as well as `sqlite` and `postgres` as a data store.

   ```console
   # Build
   go build -o resonate

   # Start
   ./resonate serve
   ```

   Once running, you'll see log output like:

   ```console
   time=2023-01-01T00:00:00.000-00:00 level=INFO msg="starting http server" addr=0.0.0.0:8001
   time=2023-01-01T00:00:00.000-00:00 level=INFO msg="starting grpc server" addr=0.0.0.0:50051
   time=2023-01-01T00:00:00.000-00:00 level=INFO msg="starting metrics server" addr=:9090
   ```

2. **Create a Promise**

   On a separate terminal, create a durable promise.

   ```console
   # Create a promise with data, headers and tags
   resonate promises create foo --timeout 1h --data foo --header bar=bar --tag baz=baz
   ```

3. **Complete a Promise**

   Finally, complete the promise by resolving or rejecting.

   ```console
   # Resolve a promise with data and headers 
   resonate promises resolve foo --data foo --header bar=bar
   ```

   ```console
   # Reject a promise with data and headers
   resonate promises reject foo --data foo --header bar=bar
   ```

## Clients  

Resonate provides client SDKs for different programming languages to easily interact with the Resonate server and write elegant distributed async await applications. More are coming soon!

- [TypeScript SDK](https://github.com/resonatehq/resonate-sdk-ts)

## Development

```console
go run ./...
go test -v ./...
```

## Contributing

See our [contribution guidelines](CONTRIBUTING.md).

## License

The Resonate Server is available under the [Apache 2.0 License](LICENSE).
