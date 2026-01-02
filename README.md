![resonate component banner](/assets/resonate-component.png)

# Resonate Server

[![ci](https://github.com/resonatehq/resonate/actions/workflows/cicd.yaml/badge.svg)](https://github.com/resonatehq/resonate/actions/workflows/cicd.yaml)
[![dst](https://github.com/resonatehq/resonate/actions/workflows/dst.yaml/badge.svg)](https://github.com/resonatehq/resonate/actions/workflows/dst.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/resonatehq/resonate)](https://goreportcard.com/report/github.com/resonatehq/resonate)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## About this component

The Resonate Server acts as a supervisor and orchestrator for Resonate Workers — that is, it provides reliability and scalability to applications built with a Resonate SDK.

**Why does this component exist?**

Popular programming languages like Python and TypeScript are not designed with abstractions that address distribution.
To provide those abstractions at the application level (i.e. to enable API-like abstractions that simplify the implementation of reliable process to process message passing) the system needs a service that acts as both an orchestrator of messages and a supervisor of the processes sending them.

The Resonate Server is a highly efficient single binary that pairs with a Resonate SDK to provide those APIs.

### [How to contribute to this repo](./CONTRIBUTING.md)

### [Get started with Resonate](https://docs.resonatehq.io/get-started/)

### [Evaluate Resonate for your next project](https://docs.resonatehq.io/evaluate/)

### [Example application library](https://github.com/resonatehq-examples)

### [The concepts that power Resonate](https://www.distributed-async-await.io/)

### [Join the Discord](https://resonatehq.io/discord)

### [Subscribe to the Blog](https://journal.resonatehq.io/subscribe)

### [Follow on Twitter](https://twitter.com/resonatehqio)

### [Follow on LinkedIn](https://www.linkedin.com/company/resonatehqio)

### [Subscribe on YouTube](https://www.youtube.com/@resonatehqio)

## Server quick start instructions

For more Resonate Server deployment information see the [Set up and run a Resonate Server](https://docs.resonatehq.io/operate/run-server) guide.

## Install with Homebrew

You can download and install the Resonate Server using Homebrew with the following commands:

```shell
brew install resonatehq/tap/resonate
```

This previous example installs the latest release.
You can see all available releases and associated release artifacts on the [releases page](https://github.com/resonatehq/resonate/releases).

Once installed, you can start the server with:

```shell
resonate serve
```

You will see log output like the following:

```shell
time=2025-09-09T20:54:31.349-06:00 level=INFO msg="starting http server" addr=:8001
time=2025-09-09T20:54:31.349-06:00 level=INFO msg="starting poll server" addr=:8002
time=2025-09-09T20:54:31.351-06:00 level=INFO msg="starting metrics server" addr=:9090
```

The output indicates that the server has HTTP endpoints available at port 8001, a polling endpoint at port 8002, and a metrics endpoint at port 9090.

These are the default ports and can be changed via configuration.
The SDKs are all configured to use these defaults unless otherwise specified.

### Run with Docker

The Resonate Server repository contains a Dockerfile that you can use to build and run the server in a Docker container.
You can also clone the repository and start the server using Docker Compose:

```shell
git clone https://github.com/resonatehq/resonate
cd resonate
docker-compose up
```

### Build from source

If you don't have Homebrew, we recommend building from source using Go.
Run the following commands to download the repository and build the server:

```
git clone https://github.com/resonatehq/resonate
cd resonate
go build -o resonate
```

After it is built, you can compile and run it as a Go program using the following command:

```
go run main.go serve
```

Or, you can run it as an executable using the following command:

```
./resonate server
```
