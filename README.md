<p align="center">
    <img height="170"src="./docs/img/echo.png">
</p>

<h1 align="center">Resonate</h1>

<div align="center">

[![ci](https://github.com/resonatehq/resonate/actions/workflows/ci.yaml/badge.svg)](https://github.com/resonatehq/resonate/actions/workflows/ci.yaml)
[![dst](https://github.com/resonatehq/resonate/actions/workflows/dst.yaml/badge.svg)](https://github.com/resonatehq/resonate/actions/workflows/dst.yaml)
[![codecov](https://codecov.io/gh/resonatehq/resonate/branch/main/graph/badge.svg)](https://codecov.io/gh/resonatehq/resonate)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

</div>

<div align="center">
  <a href="https://twitter.com/resonatehqio">Twitter</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="resonatehqcommunity.slack.com">Slack</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/resonatehq/resonate/issues">Issues</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/resonatehq/resonate/issues/131">Roadmap</a>
  <br />
</div>

### [Read the docs →](https://docs.resonatehq.io/)

## Why Resonate?
> Resonate is under active development, but already useful for improving development workflows and running simpler production code. It ships as a single executable called `Resonate`. 

Resonate offers a cloud computing model that allows you to build resilient applications using an intuitive programming interface centered around async • await. With a familiar async • await interface, you can create distributed services without learning complex failure handling techniques or vendor-specific concepts. On day one, use your existing async • await programming knowledge to build systems that recover gracefully. 

## What is a Durable Promise?
> Resonate implements the [Durable Promise API](https://github.com/resonatehq/durable-promise).

Functions & Promises have emerged as a popular model of computation, elegantly expressing concurrency and coordination. Functions represent processes and Promises represent future values. Functions & Promises compose, allowing us to build even the largest system uniformly from the smallest building blocks.

A Durable Promise is an *addressable*, *persistent* promise. A Durable Promise as defined by the [Durable Promise API](https://github.com/resonatehq/durable-promise) is a representation of a future value.

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

Refer to the [contribution guide](https://github.com/resonatehq/resonate/blob/main/CONTRIBUTING.md) to start contributing to Resonate. Your contributions will help shape the future of Durable Promises.

## Repositories

| Repo | Description |
|:-----|:------------|
| [Resonate](https://github.com/resonatehq/resonate) | The main repository that you are currently in. Contains the Resonate server code.
| [Resonate-sdk-ts](https://github.com/resonatehq/resonate-sdk-ts) | Resonate SDK for TypeScript. 
| [Docs](https://docs.resonatehq.io) | The documentation for Resonate.

## License

The Resonate Server is available under the [Apache 2.0 License](LICENSE).

---

Build reliable and scalable applications with a delightful developer experience.
