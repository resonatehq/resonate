# Kafka API

The Kafka API subsystem provides a request-response interface for Resonate over Apache Kafka. It uses a consumer group pattern with application-level target filtering for multi-tenancy and dynamic reply routing.

## Architecture

The Kafka API implements the following design:

1. **Consumer Group**: Multiple server instances can share the load by joining the same consumer group
2. **Target Filtering**: Each server has a configurable `target` identifier and filters messages at the application level
3. **Dynamic Reply Routing**: Clients specify the reply topic, target, and optional partition key in the `replyTo` field
4. **Request-Response Pattern**: Uses correlation IDs to match requests with responses

This architecture enables:
- **Multi-tenancy**: Multiple logical servers on the same topic
- **Load balancing**: Via Kafka consumer groups
- **Flexible routing**: Clients control where replies are sent
- **Partition control**: Clients can specify partition keys for ordering

## Prerequisites

### Option 1: Install Redpanda (Recommended)

Redpanda is a Kafka-compatible streaming platform that's simpler to set up (no Zookeeper required).

#### macOS (using Homebrew)
```bash
brew install redpanda-data/tap/redpanda
```

#### Start Redpanda
```bash
# Start Redpanda in dev mode
rpk redpanda start --mode dev-container
```

Or using Docker:
```bash
docker run -d --pull=always --name=redpanda-1 \
  -p 9092:9092 -p 9644:9644 \
  docker.redpanda.com/redpandadata/redpanda:latest \
  redpanda start --mode dev-container --overprovisioned
```

### Option 2: Install Kafka

#### macOS (using Homebrew)
```bash
brew install kafka
```

#### Start Kafka
```bash
# Start Zookeeper (required by Kafka)
brew services start zookeeper

# Start Kafka broker
brew services start kafka
```

### Install kcat (Kafka CLI tool)

#### macOS
```bash
brew install kcat
```

Or use Redpanda's rpk CLI (included with Redpanda):
```bash
# Already included if you installed Redpanda via Homebrew
rpk --help
```

## Configuration

The Kafka API subsystem can be configured with the following flags:

| Flag | Description | Default |
|------|-------------|---------|
| `--api-kafka-enable` | Enable the Kafka API subsystem | `false` |
| `--api-kafka-brokers` | Kafka broker addresses (comma-separated) | `localhost:9092` |
| `--api-kafka-topic` | Topic to consume requests from | `resonate.requests` |
| `--api-kafka-target` | Target identifier for this server | `resonate.server` |
| `--api-kafka-consumer-group` | Kafka consumer group | `resonate-servers` |
| `--api-kafka-timeout` | Graceful shutdown timeout | `10s` |

## Message Format

### Request Message

```json
{
  "target": "resonate.server",
  "replyTo": {
    "topic": "resonate.replies",
    "target": "my-client",
    "partitionKey": "optional-key"
  },
  "correlationId": "unique-correlation-id",
  "operation": "promises.create",
  "requestId": "optional-request-id",
  "metadata": {
    "traceparent": "optional-trace-context"
  },
  "payload": {
    "id": "promise-id",
    "timeout": 3600000,
    "param": {"value": null},
    "tags": {}
  }
}
```

**Fields:**
- `target`: Target server identifier. Messages are filtered by this field.
- `replyTo`: Where to send the reply
  - `topic`: Kafka topic for replies
  - `target`: Target identifier for the client (included in response)
  - `partitionKey`: Optional partition key for ordering
- `correlationId`: Unique ID to match request with response (required)
- `operation`: Operation to perform (e.g., "promises.create")
- `requestId`: Optional application-level request ID for tracing
- `metadata`: Optional metadata (e.g., distributed tracing headers)
- `payload`: Operation-specific payload

### Response Message

**Success:**
```json
{
  "target": "my-client",
  "correlationId": "unique-correlation-id",
  "success": true,
  "response": {
    "id": "promise-id",
    "state": "PENDING",
    "timeout": 3600000,
    "param": {"value": null},
    "value": {"value": null},
    "tags": {},
    "createdOn": 1234567890
  }
}
```

**Error:**
```json
{
  "target": "my-client",
  "correlationId": "unique-correlation-id",
  "success": false,
  "error": {
    "code": 400,
    "message": "validation error: id is required"
  }
}
```

## Available Operations

### Promises

- `promises.read` - Read a promise by ID
- `promises.search` - Search promises with filters
- `promises.create` - Create a new promise
- `promises.createtask` - Create a promise and task together
- `promises.complete` - Complete (resolve/reject) a promise
- `promises.callback` - Create a callback
- `promises.subscribe` - Create a subscription

### Schedules

- `schedules.read` - Read a schedule by ID
- `schedules.search` - Search schedules with filters
- `schedules.create` - Create a new schedule
- `schedules.delete` - Delete a schedule

### Locks

- `locks.acquire` - Acquire a distributed lock
- `locks.release` - Release a distributed lock
- `locks.heartbeat` - Send heartbeat for locks

### Tasks

- `tasks.claim` - Claim a task
- `tasks.complete` - Complete a task
- `tasks.drop` - Drop a task
- `tasks.heartbeat` - Send heartbeat for tasks

## Testing

### 1. Start Resonate with Kafka enabled

```bash
./resonate dev --api-kafka-enable
```

### 2. Create topics

**Using rpk (Redpanda):**
```bash
# Create request topic
rpk topic create resonate.requests

# Create reply topic
rpk topic create resonate.replies
```

**Using kafka-topics (Kafka):**
```bash
# Create request topic
kafka-topics --create --topic resonate.requests --bootstrap-server localhost:9092

# Create reply topic
kafka-topics --create --topic resonate.replies --bootstrap-server localhost:9092
```

### 3. Set up reply consumer (in a separate terminal)

**Using rpk (Redpanda):**
```bash
rpk topic consume resonate.replies --format '%v\n---\n'
```

**Using kcat:**
```bash
kcat -C -b localhost:9092 -t resonate.replies -f 'Partition: %p, Offset: %o\n%s\n---\n'
```

### 4. Send requests

**Note**: The examples below use `rpk topic produce`. If you're using kcat instead, replace:
- `rpk topic produce resonate.requests` with `kcat -P -b localhost:9092 -t resonate.requests`

#### Example 1: Create a Promise

**Using rpk (Redpanda):**
```bash
echo '{
  "target": "resonate.server",
  "replyTo": {
    "topic": "resonate.replies",
    "target": "my-client"
  },
  "correlationId": "req-001",
  "operation": "promises.create",
  "payload": {
    "id": "my-promise-1",
    "timeout": 3600000,
    "param": {"value": null},
    "tags": {}
  }
}' | rpk topic produce resonate.requests
```

**Using kcat:**
```bash
echo '{
  "target": "resonate.server",
  "replyTo": {
    "topic": "resonate.replies",
    "target": "my-client"
  },
  "correlationId": "req-001",
  "operation": "promises.create",
  "payload": {
    "id": "my-promise-1",
    "timeout": 3600000,
    "param": {"value": null},
    "tags": {}
  }
}' | kcat -P -b localhost:9092 -t resonate.requests
```

**Expected response:**
```json
{
  "target": "my-client",
  "correlationId": "req-001",
  "success": true,
  "response": {
    "id": "my-promise-1",
    "state": "PENDING",
    "timeout": 3600000,
    "param": {"value": null},
    "value": {"value": null},
    "tags": {},
    "createdOn": 1234567890
  }
}
```

#### Example 2: Read a Promise

```bash
echo '{
  "target": "resonate.server",
  "replyTo": {
    "topic": "resonate.replies",
    "target": "my-client"
  },
  "correlationId": "req-002",
  "operation": "promises.read",
  "payload": {
    "id": "my-promise-1"
  }
}' | rpk topic produce resonate.requests
```

#### Example 3: Search Promises

```bash
echo '{
  "target": "resonate.server",
  "replyTo": {
    "topic": "resonate.replies",
    "target": "my-client"
  },
  "correlationId": "req-003",
  "operation": "promises.search",
  "payload": {
    "id": "*",
    "state": "PENDING",
    "tags": {},
    "limit": 10
  }
}' | rpk topic produce resonate.requests
```

#### Example 4: Complete a Promise (Resolve)

```bash
echo '{
  "target": "resonate.server",
  "replyTo": {
    "topic": "resonate.replies",
    "target": "my-client"
  },
  "correlationId": "req-004",
  "operation": "promises.complete",
  "payload": {
    "id": "my-promise-1",
    "state": "RESOLVED",
    "value": {"value": "success"}
  }
}' | rpk topic produce resonate.requests
```

#### Example 5: Create a Schedule

```bash
echo '{
  "target": "resonate.server",
  "replyTo": {
    "topic": "resonate.replies",
    "target": "my-client"
  },
  "correlationId": "req-005",
  "operation": "schedules.create",
  "payload": {
    "id": "my-schedule",
    "description": "Run every hour",
    "cron": "0 * * * *",
    "promiseId": "scheduled-promise-*",
    "promiseTimeout": 3600000,
    "promiseParam": {"value": null},
    "tags": {}
  }
}' | rpk topic produce resonate.requests
```

#### Example 6: Acquire a Lock

```bash
echo '{
  "target": "resonate.server",
  "replyTo": {
    "topic": "resonate.replies",
    "target": "my-client"
  },
  "correlationId": "req-006",
  "operation": "locks.acquire",
  "payload": {
    "resourceId": "my-resource",
    "processId": "process-1",
    "executionId": "exec-1",
    "expiryInMilliseconds": 60000
  }
}' | rpk topic produce resonate.requests
```

#### Example 7: Create Promise and Task

```bash
echo '{
  "target": "resonate.server",
  "replyTo": {
    "topic": "resonate.replies",
    "target": "my-client"
  },
  "correlationId": "req-007",
  "operation": "promises.createtask",
  "payload": {
    "id": "promise-with-task",
    "timeout": 3600000,
    "param": {"value": null},
    "tags": {},
    "taskProcessId": "worker-1",
    "taskTtl": 30000,
    "taskTimeout": 60000
  }
}' | rpk topic produce resonate.requests
```

#### Example 8: Test Target Filtering

Send a message with wrong target (should be silently discarded):

```bash
echo '{
  "target": "wrong.server",
  "replyTo": {
    "topic": "resonate.replies",
    "target": "my-client"
  },
  "correlationId": "req-008",
  "operation": "promises.read",
  "payload": {
    "id": "my-promise-1"
  }
}' | rpk topic produce resonate.requests
```

You should see a debug log in Resonate's output indicating the message was discarded due to target mismatch.

#### Example 9: Test Partition Key

Send a message with partition key for ordering:

```bash
echo '{
  "target": "resonate.server",
  "replyTo": {
    "topic": "resonate.replies",
    "target": "my-client",
    "partitionKey": "user-123"
  },
  "correlationId": "req-009",
  "operation": "promises.create",
  "payload": {
    "id": "user-123-promise",
    "timeout": 3600000,
    "param": {"value": null},
    "tags": {}
  }
}' | rpk topic produce resonate.requests
```

The reply will be sent to the same partition as other messages with "user-123" key.

#### Example 10: Error Handling

Test with invalid payload:

```bash
echo '{
  "target": "resonate.server",
  "replyTo": {
    "topic": "resonate.replies",
    "target": "my-client"
  },
  "correlationId": "req-010",
  "operation": "promises.create",
  "payload": {
    "timeout": 3600000
  }
}' | rpk topic produce resonate.requests
```

**Expected error response:**
```json
{
  "target": "my-client",
  "correlationId": "req-010",
  "success": false,
  "error": {
    "code": 400,
    "message": "validation error: id is required"
  }
}
```

## Multi-Tenancy Example

The target filtering enables multi-tenancy on a single topic. Start multiple Resonate instances:

```bash
# Server 1
./resonate dev --api-kafka-enable --api-kafka-target server1

# Server 2 (in another terminal)
./resonate dev --api-kafka-enable --api-kafka-target server2 --api-http-addr :9001 --api-grpc-addr :50052
```

Send requests to different servers:

```bash
# To server1
echo '{
  "target": "server1",
  "replyTo": {"topic": "resonate.replies", "target": "client"},
  "correlationId": "req-s1",
  "operation": "promises.create",
  "payload": {"id": "promise-s1", "timeout": 3600000, "param": {"value": null}, "tags": {}}
}' | rpk topic produce resonate.requests

# To server2
echo '{
  "target": "server2",
  "replyTo": {"topic": "resonate.replies", "target": "client"},
  "correlationId": "req-s2",
  "operation": "promises.create",
  "payload": {"id": "promise-s2", "timeout": 3600000, "param": {"value": null}, "tags": {}}
}' | rpk topic produce resonate.requests
```

## Message Schemas

### Promise Operations

#### promises.create
```json
{
  "id": "string",
  "idempotencyKey": "string (optional)",
  "strict": false,
  "param": {"headers": {}, "data": null},
  "timeout": 3600000,
  "tags": {"key": "value"}
}
```

#### promises.complete
```json
{
  "id": "string",
  "idempotencyKey": "string (optional)",
  "strict": false,
  "state": "RESOLVED|REJECTED|REJECTED_CANCELED|REJECTED_TIMEDOUT",
  "value": {"headers": {}, "data": null}
}
```

### Schedule Operations

#### schedules.create
```json
{
  "id": "string",
  "description": "string (optional)",
  "cron": "0 * * * *",
  "tags": {"key": "value"},
  "promiseId": "string",
  "promiseTimeout": 3600000,
  "promiseParam": {"headers": {}, "data": null},
  "promiseTags": {"key": "value"},
  "idempotencyKey": "string (optional)"
}
```

### Lock Operations

#### locks.acquire
```json
{
  "resourceId": "string",
  "processId": "string",
  "executionId": "string",
  "expiryInMilliseconds": 60000
}
```

### Task Operations

#### tasks.claim
```json
{
  "id": "string",
  "counter": 0,
  "processId": "string",
  "ttl": 30000
}
```

## Performance Considerations

1. **Consumer Group**: Use the same consumer group for all instances that should share the load
2. **Partitions**: Increase topic partitions to improve parallelism
3. **Batch Size**: Tune Kafka consumer batch settings for throughput
4. **Target Filtering**: Messages with wrong targets are filtered early (after Kafka consumption but before processing)
5. **Reply Partitioning**: Use partition keys in replyTo for ordered replies

## Troubleshooting

### No response received

1. Check if reply consumer is running
2. Verify `replyTo.topic` exists
3. Check `correlationId` matches between request and response
4. Check Resonate server logs for errors

### Message not processed

1. Verify `target` matches server configuration
2. Check Resonate server is running with `--api-kafka-enable`
3. Check consumer group membership:
   - With rpk: `rpk group describe resonate-servers`
   - With kafka-consumer-groups: `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group resonate-servers`

### Consumer lag

1. Check consumer group lag:
   - With rpk: `rpk group describe resonate-servers`
   - With kafka-consumer-groups: `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group resonate-servers`
2. Add more Resonate instances to the consumer group
3. Increase topic partitions

## Development Notes

- The Kafka API uses the IBM Sarama library (github.com/IBM/sarama)
- Consumer group rebalancing is handled automatically
- Message offsets are committed after processing
- All 21 operations from HTTP/gRPC APIs are supported
- Target filtering happens at application level (after Kafka delivery)
