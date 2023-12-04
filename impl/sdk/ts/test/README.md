# Resonate Server Tests

Resonate Server Tests is a test suite for validating the functionality of your Resonate server implementation. The tests are written using Jest, a popular JavaScript testing framework.

## Prerequisites

Before running the tests, make sure you have the following dependencies installed:

- Node.js: You need Node.js installed on your machine.

## Getting Started

1. Clone the repository to your local machine.

2. Install project dependencies:

   ```bash
   npm install
   ```

3. Run the tests:

   ```bash
   npm test
   ```

   This command will run the Jest test suite and execute the test cases.

   In case you want to run individual test cases, you can use the following command:

   ```bash
   npx jest tests/resonate.test.ts --verbose
   ```

4. Optional: Set environment variables to configure the tests:

   - `BINARY_PATH`: Set this variable to specify the path to the Resonate binary executable. If not set, it falls back to a default path.

   - `RESONATE_URL`: Set this variable to specify the URL of the Resonate server. If not set, it falls back to a default URL. (default: `http://localhost:8001`)

   - `USE_DURABLE`: Set this variable to `'true'` if you want to use the DurablePromiseStore, or `'false'` to use the VolatilePromiseStore. By default, the tests use the VolatilePromiseStore.

   Example:

   ```bash
   BINARY_PATH=/path/to/resonate-binary USE_DURABLE=true npm test
   ```

## Test Cases

The test suite includes various test cases to verify different aspects of the Resonate server's behavior, including:

- Creating promises
- Resolving promises
- Handling duplicate create requests
- Rejecting promises
- Cancelling promises
- Handling duplicate resolve and reject requests
- Strict mode behavior

Feel free to customize and extend the test cases to suit your specific requirements.

## Environment Variables

- `BINARY_PATH`: Path to the Resonate binary executable.
- `USE_DURABLE`: Set to `'true'` to use DurablePromiseStore, or `'false'` to use VolatilePromiseStore (default: `'false'`).