# Resonate end-to-end test harness 

Harness is a verification system that checks implementations for conformance to the [Durable Promise Specification](https://github.com/resonatehq/durable-promise). 


In Harness, integration tests are called 'simulations'. A simulation is controlled by a program (the 'simulator'). The simulator launches clients and contains test logic. It reports results back and are aggregated for display in a web browser. 
perspective. 

1. A test runs as a Golang program. That program setups the implementation you're going to
test (right now just resonate server).  

2. Once the *system* is running, the harness spins up a set of logically single-threaded processes, each with its own client for the distributed system. 

3. A *generator* generates new operations for each process to perform. 

4. Processes then apply those operations to the system using their clients. The start and end of each operation is recorded in a history. 

5. Finally the implementation are torn down. 

6. Harness uses a *checker* to analyze the test's history for correctness, and to generate repots, graphs, etc. 

NOTE: the test, history, analysis, and any supplementary results are written to the filesystem under store/<test-name>/<date> for later review. Symlinks to the latest results are maintained at each level for convenience. 

<diagram>

## Writing a test


## Running Tests

Harness offer two simulation types: 

- single client: establishing correctness 
- multiple clients: load testing, offers benchmarks

```bash
go test -v ./... ] 
```

## Contributions

Configurable to test harness -- docker compose 
