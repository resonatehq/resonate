default:
    just --list


build:
    ./gradlew build

test:
    ./gradlew test

# Apply code formatting
format:
    ./gradlew spotlessApply

# Check formatting without modifying files
lint:
    ./gradlew spotlessCheck

# Run every example against a Resonate server on localhost:8001 (`resonate dev`)
examples:
    ./gradlew runExample -PmainClass=io.resonatehq.examples.helloworld.HelloWorld
    ./gradlew runExample -PmainClass=io.resonatehq.examples.fibonacci.Fibonacci -PexampleArgs="--mode run --n 12"
    ./gradlew runExample -PmainClass=io.resonatehq.examples.fibonacci.Fibonacci -PexampleArgs="--mode rpc --n 12"
    ./gradlew runExample -PmainClass=io.resonatehq.examples.fibonacci.Fibonacci -PexampleArgs="--mode mix --n 12"
    ./gradlew runExample -PmainClass=io.resonatehq.examples.errorhandling.ErrorHandling -PexampleArgs="--mode run --error none"
    ./gradlew runExample -PmainClass=io.resonatehq.examples.errorhandling.ErrorHandling -PexampleArgs="--mode run --error taken"
    ./gradlew runExample -PmainClass=io.resonatehq.examples.errorhandling.ErrorHandling -PexampleArgs="--mode run --error value"
    ./gradlew runExample -PmainClass=io.resonatehq.examples.errorhandling.ErrorHandling -PexampleArgs="--mode rpc --error none"
    ./gradlew runExample -PmainClass=io.resonatehq.examples.errorhandling.ErrorHandling -PexampleArgs="--mode rpc --error taken"
    ./gradlew runExample -PmainClass=io.resonatehq.examples.errorhandling.ErrorHandling -PexampleArgs="--mode rpc --error value"
    ./gradlew runExample -PmainClass=io.resonatehq.examples.pipeline.Pipeline
    ./gradlew runExample -PmainClass=io.resonatehq.examples.rpc.Rpc
    ./gradlew runExample -PmainClass=io.resonatehq.examples.saga.Saga
    ./gradlew runExample -PmainClass=io.resonatehq.examples.saga.Saga -PexampleArgs="--fail hotel"
    ./gradlew runExample -PmainClass=io.resonatehq.examples.saga.Saga -PexampleArgs="--fail charge"
    ./gradlew runExample -PmainClass=io.resonatehq.examples.versioning.Versioning
    ./gradlew runExample -PmainClass=io.resonatehq.examples.humanintheloop.HumanInTheLoop -PexampleArgs="--decision approve"
    ./gradlew runExample -PmainClass=io.resonatehq.examples.humanintheloop.HumanInTheLoop -PexampleArgs="--decision reject"
    ./gradlew runExample -PmainClass=io.resonatehq.examples.recovery.Recovery
    ./gradlew runExample -PmainClass=io.resonatehq.examples.detached.Detached
    ./gradlew runExample -PmainClass=io.resonatehq.examples.polling.Polling
    ./gradlew runExample -PmainClass=io.resonatehq.examples.structuredconcurrency.StructuredConcurrency
    ./gradlew runExample -PmainClass=io.resonatehq.examples.retries.Retries
