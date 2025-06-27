# Networking Interface for Distributed Async Await TypeScript SDK

## Highlight

The Resonate Distributed Async Await TypeScript SDK assumes that the network is inherently unreliable, reflecting the uncertainty of communication in distributed systems. Conventional system design responds to uncertainty with complexity, layering retries, surfacing status and error codes, offering information that appears actionable, yet is often misleading. Resonate's design responds to complexity with simplicity.

By grounding the design in the weakest communication assumptions, the SDK achieves maximum reliability and flexibility. The SDK guarantees correctness without relying on properties such as

- at-most once delivery or processing,
- at-least once delivery or processing, or
- message ordering.

As a result, the SDK can operate on a wide range of transports such as TCP, UDP, or message queues, either push- or poll-based.

## Background

Networked communication introduces uncertainty. When a source sends a message to a target, the source introduces uncertainty to its knowledge of the target. The source does not know whether the message was sent, received, or (most importantly) processed.

From the source's perspective, the target is in a state of superposition: The target may have processed the message or may not have processed the message. Only when the source receives a message from the target in response, the source reduces uncertainty from its knowledge of the target. A response confirms that the message was sent, received, and (assuming the target is informative and honest), processed.

A core source of complexity in distributed systems is this lingering uncertainty when no response is received. At that point, if desired, the source must find alternative ways to reduce uncertainty, for example, by retrying until a response arrives.

## Architecture & Interface Semantics

The TypeScript SDK adopts *message passing* as its fundamental networking abstraction, exposing one interface:

```ts
function send<REQ, RES>(
  address: Address,
  message: REQ,
  timeout: number,
  callback: (timeout: boolean, message: RES) => void
): void
```

The `send` function provides a single guarantee:

```
The callback will eventually be invoked exactly once, either with a response or a timeout.
```

There are two cases:

1. **A response is received**

    The callback is invoked with `timeout: false` and a response message.

2. **No response is received**

    The callback is invoked with `timeout: true` with late response messages silently dropped.

This contract is intentionally minimal and collapses the uncertainty of networked communication into a single decision point: response or no response.

## Dimensionality Reduction

This design follows the principle of Dimensionality Reduction, externalizing a minimal amount of information to ensure minimal branching at the callsite.

To understand the principle of Dimensionality Reduction, let's consider the TCP error codes connection refused (`ECONNREFUSED`) and connection reset (`ECONNRESET`): Connection refused indicates that a connection was not established, while connection reset indicates that a connection was established and subsequently closed. In the first case, the message was not received and processed, while in the second case, the message may have been received and processed.

Superficially, these error codes suggest different outcomes. But from the callers’s perspective, they are equivalent in what ultimately matters: no response was received.

By not exposing error codes, the `send` function prevents the caller from branching on a distinction that appears actionable at first glance, yet is either irrelevant or misleading.

#### Irrelevant

The consequences of ECONNREFUSED (message not processed) are subsumed by the consequences of ECONNRESET (message not processed *or* message processed). In other words, the system must already be prepared to handle the more general case. Why branch on the more specific case?

#### Misleading

Receiving ECONNREFUSED might tempt the source to believe that "this message was not processed" while in actuality, the source has learned that "my attempt at this message was not processed". That is not the same and says nothing about a different component already sent or is about to send the (an equivalent) message. A definite failure at a component level does not equate a definite failure at the system level.

By not surfacing error codes, the send function avoids inviting assumptions that appear actionable but either add no value or lead to incorrect conclusions. This reduction in exposed state leads to a simpler and safer systems.
