// Package resonate is the Go SDK for Resonate's durable execution platform.
//
// Workflow functions execute from the top on every replay. Keep observable side
// effects such as logging, HTTP calls, and metric increments inside [Context.Run]
// so already-settled child promises can be short-circuited on replay.
//
// This package owns the workflow API ([Context], [Effects], [Run], [RPC],
// [Sleep], [Promise], [Detached]), the direct promise and schedule API
// ([Resonate.Promises], [Resonate.Schedules]), the wire protocol ([Sender],
// [Network] interface, push-message decoding), and the shared domain types
// ([PromiseRecord], [TaskRecord], etc.). Concrete transport implementations
// live in leaf subpackages:
//
//   - [github.com/resonatehq/resonate-sdk-go/httpnet]: HTTP + SSE transport
//     for talking to a live Resonate server.
//   - [github.com/resonatehq/resonate-sdk-go/localnet]: In-process transport
//     that runs the server state machine in a single actor goroutine. Useful
//     for tests and "no-server-required" local development.
package resonate
