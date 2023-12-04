# Resonate Example Web Application

This is an example web application that demonstrates how to use Resonate, a library for durable promises. 

It features an Express server that allows you to call functions and handle retries seamlessly.

## Getting Started

The following instructions will help you get started with the project on your local machine.

### Prerequisites

Before you begin, make sure you have the following software installed:

- Node.js
- npm (Node Package Manager)

### Installation

1. Clone this repository to your local machine.

   ```bash
   git clone git@github.com:resonatehq/durable-promise-ts.git
   ```

2. Change to the project's directory.

   ```bash
   cd durable-promise-ts/example/web
   ```

3. Install project dependencies.

   ```bash
   npm install
   ```

### Running the Application

1. Start the application.

   ```bash
   npm start
   ```

2. The Express server will start and run on port 8080. You can access the Resonate functions via HTTP POST requests to `http://localhost:8080/:name/:id`, where `:name` is the function name and `:id` is the function identifier.

3. To trace function calls and errors, you can use Jaeger tracing by running the following command:

   ```bash
   npm run trace
   ```

## Usage

You can use this example application to call functions orchestrated by Resonate via HTTP POST requests. The server listens on port 8080 and handles function retries and errors.

Example usage with `curl`:

```bash
curl -X POST -H "Content-Type: application/json" -d '{"value": 42}' http://localhost:8080/foo/123
```

In this example, `foo` is the function name, and `123` is the function identifier. You can replace these values with your own function names and identifiers.

## Guarantees

Resonate provides the following guarantees:

`Volatile Promise Store`
- Functions using Volatile Promise Store provide high performance with lower durability guarantees.
- Promises created using Volatile Promise Store will be lost if the server crashes.

`Durable Promise Store`
- Functions using Durable Promise Store provide strong durability guarantees.
- Promises created using Durable Promise Store are persisted even if the server crashes, ensuring data integrity.