# NATS API Subsystem

This subsystem implements a NATS.io-based API for Resonate using the standard request-reply pattern.

## Overview

The NATS API provides access to all Resonate operations through a single NATS subject, with operation routing based on the message payload. This enables distributed communication between Resonate servers and clients using the NATS messaging system.

## Architecture

### Single Subject Design

All operations are sent to a single, configurable NATS subject (default: `resonate.server`). The operation type is specified in the message payload's `operation` field.

**Benefits:**
- Simple configuration (one subject to manage)
- Easy to multiplex multiple Resonate servers (each can have its own subject)
- Traditional RPC-style interface
- Single subscription per server instance

### Message Structure

All messages follow this JSON structure:

```json
{
  "operation": "<resource>.<action>",
  "requestId": "optional-request-id",
  "metadata": {
    "key": "value"
  },
  "payload": {
    // Operation-specific payload
  }
}
```

**Fields:**

- `operation` **(required)**: The operation to perform (e.g., `promises.read`, `schedules.create`)
- `requestId` *(optional)*: Unique identifier for this request (application-level tracking). Auto-generated if not provided. Note: NATS handles reply routing automatically at the protocol level.
- `metadata` *(optional)*: Key-value pairs for protocol metadata (e.g., tracing headers)
- `payload` **(required)**: Operation-specific data

## Configuration

### Command Line Flags

Enable and configure the NATS API using the following flags:

```bash
resonate serve \
  --api-nats-enable=true \
  --api-nats-addr=nats://localhost:4222 \
  --api-nats-subject=resonate.server \
  --api-nats-timeout=10s
```

### Configuration File

Add to `resonate.yaml`:

```yaml
api:
  subsystems:
    nats:
      enabled: true
      addr: "nats://localhost:4222"
      subject: "resonate.server"
      timeout: "10s"
```

### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `enabled` | Enable NATS API subsystem | `false` |
| `addr` | NATS server address | `nats://localhost:4222` |
| `subject` | NATS subject name for all operations | `resonate.server` |
| `timeout` | Graceful shutdown timeout | `10s` |

### Multiple Server Setup

To run multiple Resonate instances with independent NATS APIs:

```yaml
# Server 1
api:
  subsystems:
    nats:
      enabled: true
      subject: "resonate.server1"

# Server 2
api:
  subsystems:
    nats:
      enabled: true
      subject: "resonate.server2"
```

Clients can then target specific servers by sending requests to the appropriate subject.

## Supported Operations

### Promises
- `promises.read` - Read a promise by ID
- `promises.search` - Search promises with filters
- `promises.create` - Create a new promise
- `promises.createtask` - Create a promise with an associated task
- `promises.complete` - Complete a promise (resolve/reject/cancel)
- `promises.callback` - Create a callback for a promise
- `promises.subscribe` - Create a subscription for a promise

### Schedules
- `schedules.read` - Read a schedule by ID
- `schedules.search` - Search schedules with filters
- `schedules.create` - Create a new schedule
- `schedules.delete` - Delete a schedule

### Locks
- `locks.acquire` - Acquire a distributed lock
- `locks.release` - Release a distributed lock
- `locks.heartbeat` - Send heartbeat for active locks

### Tasks
- `tasks.claim` - Claim a task for execution
- `tasks.complete` - Complete a claimed task
- `tasks.drop` - Drop a claimed task
- `tasks.heartbeat` - Send heartbeat for active tasks

## Response Format

### Successful Response

```json
{
  "success": true,
  "response": {
    // Operation-specific response data
  }
}
```

### Error Response

```json
{
  "success": false,
  "error": {
    "code": 400,
    "message": "Error description"
  }
}
```

### Error Codes

- `400` - Bad Request (validation errors, invalid payload, unknown operation)
- `404` - Not Found (resource doesn't exist)
- `409` - Conflict (resource already exists, state conflicts)
- `500` - Internal Server Error (server-side errors)

## Usage Examples

### Using NATS CLI

#### Read a Promise

```bash
nats req resonate.server '{
  "operation": "promises.read",
  "payload": {
    "id": "my-promise-id"
  }
}'
```

#### Create a Promise

```bash
nats req resonate.server '{
  "operation": "promises.create",
  "requestId": "req-123",
  "payload": {
    "id": "my-promise",
    "timeout": 3600000,
    "param": {
      "headers": {},
      "data": null
    },
    "tags": {
      "environment": "production"
    }
  }
}'
```

#### Search Promises

```bash
nats req resonate.server '{
  "operation": "promises.search",
  "payload": {
    "id": "my-promise-*",
    "states": ["PENDING", "RESOLVED"],
    "tags": {},
    "limit": 10
  }
}'
```

#### Complete a Promise (Resolve)

```bash
nats req resonate.server '{
  "operation": "promises.complete",
  "payload": {
    "id": "my-promise",
    "state": "RESOLVED",
    "value": {
      "headers": {"content-type": "application/json"},
      "data": "eyJyZXN1bHQiOiAic3VjY2VzcyJ9"
    }
  }
}'
```

#### Create a Schedule

```bash
nats req resonate.server '{
  "operation": "schedules.create",
  "payload": {
    "id": "daily-report",
    "cron": "0 0 * * *",
    "promiseId": "report-{{.timestamp}}",
    "promiseTimeout": 3600000,
    "promiseParam": {
      "headers": {},
      "data": null
    }
  }
}'
```

### Using Go Client

```go
package main

import (
    "encoding/json"
    "fmt"
    "time"

    "github.com/nats-io/nats.go"
)

func main() {
    // Connect to NATS
    nc, err := nats.Connect("nats://localhost:4222")
    if err != nil {
        panic(err)
    }
    defer nc.Close()

    // Create a promise request
    request := map[string]interface{}{
        "operation": "promises.create",
        "payload": map[string]interface{}{
            "id":      "my-promise",
            "timeout": 3600000,
            "param": map[string]interface{}{
                "headers": map[string]string{},
                "data":    nil,
            },
        },
    }

    requestBytes, _ := json.Marshal(request)

    // Send request and wait for response
    msg, err := nc.Request("resonate.server", requestBytes, 5*time.Second)
    if err != nil {
        panic(err)
    }

    // Parse response
    var response map[string]interface{}
    json.Unmarshal(msg.Data, &response)

    if response["success"].(bool) {
        fmt.Println("Promise created successfully!")
        fmt.Printf("Response: %+v\n", response["response"])
    } else {
        fmt.Printf("Error: %+v\n", response["error"])
    }
}
```

### Using Python Client

```python
import json
import asyncio
from nats.aio.client import Client as NATS

async def main():
    # Connect to NATS
    nc = NATS()
    await nc.connect("nats://localhost:4222")

    # Create a promise request
    request = {
        "operation": "promises.create",
        "payload": {
            "id": "my-promise",
            "timeout": 3600000,
            "param": {
                "headers": {},
                "data": None
            }
        }
    }

    # Send request and wait for response
    response = await nc.request(
        "resonate.server",
        json.dumps(request).encode(),
        timeout=5
    )

    # Parse response
    result = json.loads(response.data)

    if result["success"]:
        print("Promise created successfully!")
        print(f"Response: {result['response']}")
    else:
        print(f"Error: {result['error']}")

    await nc.close()

if __name__ == '__main__':
    asyncio.run(main())
```

### Using JavaScript/Node.js Client

```javascript
const { connect, StringCodec } = require('nats');

async function main() {
    // Connect to NATS
    const nc = await connect({ servers: 'nats://localhost:4222' });
    const sc = StringCodec();

    // Create a promise request
    const request = {
        operation: 'promises.create',
        payload: {
            id: 'my-promise',
            timeout: 3600000,
            param: {
                headers: {},
                data: null
            }
        }
    };

    // Send request and wait for response
    const response = await nc.request(
        'resonate.server',
        sc.encode(JSON.stringify(request)),
        { timeout: 5000 }
    );

    // Parse response
    const result = JSON.parse(sc.decode(response.data));

    if (result.success) {
        console.log('Promise created successfully!');
        console.log('Response:', result.response);
    } else {
        console.log('Error:', result.error);
    }

    await nc.close();
}

main();
```

## Request Payload Schemas

### promises.read

```json
{
  "operation": "promises.read",
  "payload": {
    "id": "promise-id"
  }
}
```

### promises.create

```json
{
  "operation": "promises.create",
  "payload": {
    "id": "promise-id",
    "idempotencyKey": "optional-idempotency-key",
    "strict": false,
    "param": {
      "headers": {},
      "data": null
    },
    "timeout": 3600000,
    "tags": {
      "key": "value"
    }
  }
}
```

### promises.complete

```json
{
  "operation": "promises.complete",
  "payload": {
    "id": "promise-id",
    "idempotencyKey": "optional-idempotency-key",
    "strict": false,
    "state": "RESOLVED",
    "value": {
      "headers": {},
      "data": null
    }
  }
}
```

**State options:** `RESOLVED`, `REJECTED`, `REJECTED_CANCELED`, `REJECTED_TIMEDOUT`

### promises.search

```json
{
  "operation": "promises.search",
  "payload": {
    "id": "promise-*",
    "states": ["PENDING", "RESOLVED"],
    "tags": {},
    "limit": 10,
    "cursor": "optional-cursor-for-pagination"
  }
}
```

### schedules.create

```json
{
  "operation": "schedules.create",
  "payload": {
    "id": "schedule-id",
    "description": "Optional description",
    "cron": "0 0 * * *",
    "tags": {},
    "promiseId": "promise-{{.timestamp}}",
    "promiseTimeout": 3600000,
    "promiseParam": {
      "headers": {},
      "data": null
    },
    "promiseTags": {}
  }
}
```

### locks.acquire

```json
{
  "operation": "locks.acquire",
  "payload": {
    "resourceId": "resource-id",
    "processId": "process-id",
    "executionId": "execution-id",
    "expiryInMilliseconds": 60000
  }
}
```

### tasks.claim

```json
{
  "operation": "tasks.claim",
  "payload": {
    "id": "promise-id",
    "counter": 0,
    "processId": "optional-process-id",
    "frequency": 60000
  }
}
```

### tasks.complete

```json
{
  "operation": "tasks.complete",
  "payload": {
    "id": "promise-id",
    "counter": 0,
    "state": "RESOLVED",
    "value": {
      "headers": {},
      "data": null
    }
  }
}
```

**State options:** `RESOLVED`, `REJECTED`

## Testing

### Prerequisites

Install the NATS CLI and server:

```bash
# Install NATS server
brew install nats-server

# Install NATS CLI
brew install nats-io/nats-tools/nats
```

### Step-by-step testing

1. **Start a NATS server:**
   ```bash
   nats-server
   ```

2. **Start Resonate with NATS enabled:**
   ```bash
   # For development (in-memory):
   ./resonate dev --api-nats-enable

   # For production:
   ./resonate serve --api-nats-enable
   ```

3. **Test with NATS CLI:**

   **Create a promise:**
   ```bash
   nats req resonate.server '{
     "operation": "promises.create",
     "requestId": "test-123",
     "payload": {
       "id": "test-promise",
       "timeout": 3600000,
       "param": {
         "headers": {},
         "data": null
       }
     }
   }'
   ```

   **Read a promise:**
   ```bash
   nats req resonate.server '{
     "operation": "promises.read",
     "payload": {
       "id": "test-promise"
     }
   }'
   ```

   **Search promises:**
   ```bash
   nats req resonate.server '{
     "operation": "promises.search",
     "payload": {
       "id": "*",
       "states": ["PENDING"],
       "tags": {},
       "limit": 10
     }
   }'
   ```

   **Complete a promise:**
   ```bash
   nats req resonate.server '{
     "operation": "promises.complete",
     "payload": {
       "id": "test-promise",
       "state": "RESOLVED",
       "value": {
         "headers": {},
         "data": null
       }
     }
   }'
   ```

   **Create a schedule:**
   ```bash
   nats req resonate.server '{
     "operation": "schedules.create",
     "payload": {
       "id": "daily-job",
       "cron": "0 0 * * *",
       "promiseId": "job-{{.timestamp}}",
       "promiseTimeout": 3600000,
       "promiseParam": {
         "headers": {},
         "data": null
       }
     }
   }'
   ```

4. **Monitor NATS traffic (optional):**
   ```bash
   # In another terminal, subscribe to see all messages
   nats sub resonate.server
   ```

5. **Check NATS server status:**
   ```bash
   nats server info
   ```

## Performance Considerations

- The NATS API uses JSON serialization, which may be slower than Protocol Buffers (gRPC)
- For high-throughput scenarios, consider using the gRPC API instead
- NATS provides excellent horizontal scalability and load balancing capabilities
- Single subject design minimizes subscription overhead

## Security

- The current implementation does not include authentication/authorization at the API level
- Consider using NATS security features for production:
  - **TLS**: Encrypt client-server communication
  - **Authentication**: Username/password, tokens, or certificates
  - **Authorization**: Subject-level permissions
  - **Accounts**: Multi-tenancy and isolation
  - **JWT**: Token-based authentication

Example secure connection:
```bash
resonate serve \
  --api-nats-addr=nats://username:password@localhost:4222 \
  --api-nats-enable=true
```

## Comparison with HTTP and gRPC APIs

| Feature | NATS | HTTP | gRPC |
|---------|------|------|------|
| Protocol | NATS messaging | REST/HTTP | HTTP/2 + Protobuf |
| Serialization | JSON | JSON | Protocol Buffers |
| Subject/Endpoint | Single subject | Multiple endpoints | Multiple RPCs |
| Load Balancing | Native NATS | External (nginx, etc.) | External or client-side |
| Pub/Sub | Native support | Not applicable | Streaming only |
| Discovery | Subject-based | URL-based | Service-based |
| Performance | High (binary protocol) | Medium (text protocol) | Highest (binary + HTTP/2) |
| Use Case | Distributed systems, microservices | Web clients, simple APIs | High-performance RPC |

## Future Enhancements

Potential improvements for future versions:

- Add support for NATS streaming patterns (publish/subscribe)
- Implement JetStream for persistent message delivery
- Add authentication and authorization support
- Support for NATS connection options (TLS, credentials, etc.)
- Queue groups for load balancing across multiple instances
- Metrics and monitoring via NATS monitoring endpoints
- Binary encoding (e.g., MessagePack) for better performance
