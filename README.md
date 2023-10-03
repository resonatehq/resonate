# Resonate

[![ci](https://github.com/resonatehq/resonate/actions/workflows/ci.yaml/badge.svg)](https://github.com/resonatehq/resonate/actions/workflows/ci.yaml)
[![dst](https://github.com/resonatehq/resonate/actions/workflows/dst.yaml/badge.svg)](https://github.com/resonatehq/resonate/actions/workflows/dst.yaml)
[![codecov](https://codecov.io/gh/resonatehq/resonate/branch/main/graph/badge.svg)](https://codecov.io/gh/resonatehq/resonate)


The Resonate Server implements the [Durable Promise API](https://docs.google.com/document/d/1l-Of-0hOm6EYze_fXWlkpEpxVRlXfYm1MDKwE0jUTZA).

## What is a Durable Promise?

Functions & Promises have emerged as a popular model of computation, elegantly expressing concurrency and coordination. Functions represent processes and Promises represent future values. Functions & Promises compose, allowing us to build even the largest system uniformly from the smallest building blocks.

A Durable Promise is an *addressable*, *persistent* promise. A Durable Promise as defined by the [Durable Promise API](https://docs.google.com/document/d/1l-Of-0hOm6EYze_fXWlkpEpxVRlXfYm1MDKwE0jUTZA) is a representation of a future value.

## Getting Started

1. **Build**

   The resonate server supports `http` and `grpc` protocols as well as `sqlite` and `postgres` as a data store.

   ```
   # build
   go build -o resonate

   # start
   ./resonate serve
   time=2023-01-01T00:00:00.000-00:00 level=INFO msg="starting http server" addr=0.0.0.0:8001
   time=2023-01-01T00:00:00.000-00:00 level=INFO msg="starting grpc server" addr=0.0.0.0:50051
   time=2023-01-01T00:00:00.000-00:00 level=INFO msg="starting metrics server" addr=:9090
   ```
2. **Create a Promise**

   Create a Durable Promise using a unique identifier and an idempotency key.

   ```bash
   curl -X POST -H "Idempotency-Key: foo_create" -d '{
     "param": {
       "data": "'"$(echo -n 'Durable Promise Created' | base64)"'"
     },
     "timeout": 2524608000000
   }' http://localhost:8001/promises/foo/create
   ```

3. **Complete a Promise**

   Resolve or reject a Durable Promise using its identifier and an idempotency key.

   ```bash
   curl -X POST -H "Idempotency-Key: foo_complete" -d '{
     "value": {
       "data": "'"$(echo -n 'Durable Promise Resolved' | base64)"'"
     }
   }' http://localhost:8001/promises/foo/resolve
   ```

   ```bash
   curl -X POST -H "Idempotency-Key: foo_complete" -d '{
     "value": {
       "data": "'"$(echo -n 'Durable Promise Rejected' | base64)"'"
     }
   }' http://localhost:8001/promises/foo/reject
   ```

## Development

```
go run ./...
go test -v ./...
```

## Get Involved

Contribute to the Resonate Server by submitting issues and pull requests. Your contributions will help shape the future of Durable Promises.

## License

The Resonate Server is available under the [Apache 2.0 License](LICENSE).

---

Build reliable and scalable applications with a delightful developer experience.
