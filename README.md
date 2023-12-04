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
> Resonate is under active development, but already useful for improving development workflows and running simpler production code. It ships as a single executable called `Resonate`. 

Resonate offers a cloud computing model that allows you to build resilient applications using an intuitive programming interface centered around async • await. With a familiar async • await interface, you can create distributed services without learning complex failure handling techniques or vendor-specific concepts. On day one, use your existing async • await programming knowledge to build systems that recover gracefully. 

## What is a Durable Promise?
> Resonate implements the [Durable Promise API](https://github.com/resonatehq/durable-promise).

Functions & Promises have emerged as a popular model of computation, elegantly expressing concurrency and coordination. Functions represent processes and Promises represent future values. Functions & Promises compose, allowing us to build even the largest system uniformly from the smallest building blocks.

A Durable Promise is an *addressable*, *persistent* promise. A Durable Promise as defined by the [Durable Promise API](https://github.com/resonatehq/durable-promise) is a representation of a future value.

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

   ```bash
   # Build
   go build -o resonate

   # Start
   ./resonate serve
   ```

   Once running, you'll see log output like: 
   ```bash
   time=2023-01-01T00:00:00.000-00:00 level=INFO msg="starting http server" addr=0.0.0.0:8001
   time=2023-01-01T00:00:00.000-00:00 level=INFO msg="starting grpc server" addr=0.0.0.0:50051
   time=2023-01-01T00:00:00.000-00:00 level=INFO msg="starting metrics server" addr=:9090
   ```

2. **Create a Promise**

   Next, create a durable promise by making a POST request with a unique identifier and an idempotency key.

   ```bash
   curl -X POST \
     -H "Idempotency-Key: foo_create" \
     -d '{
       "param": {
         "data": "'$(echo -n 'Durable Promise Created' | base64)'"
       },
       "timeout": 2524608000000
     }' \
     http://localhost:8001/promises/foo/create
   ```

3. **Complete a Promise**

   Finally, complete the promise by resolving or rejecting it Pass the same ID and a new idempotency key. 

   ```bash
   curl -X POST \
     -H "Idempotency-Key: foo_resolve" \
     -d '{
       "value": {
         "data": "'$(echo -n 'Durable Promise Resolved' | base64)'"
       }
     }' \
     http://localhost:8001/promises/foo/resolve
   ```

   ```bash
   curl -X POST \
     -H "Idempotency-Key: foo_reject" \ 
     -d '{
       "value": {
         "data": "'$(echo -n 'Durable Promise Rejected' | base64)'"
       }
     }' \
     http://localhost:8001/promises/foo/reject
   ```

## Development
```
go run ./...
go test -v ./...
```

## Contributing
See our [contribution guidelines](CONTRIBUTING.md).

## License
The Resonate Server is available under the [Apache 2.0 License](LICENSE).
