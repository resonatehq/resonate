// Package network owns the transport seam of the SDK: the Network interface,
// its two implementations (LocalNetwork for in-process tests and HTTPNetwork
// for the live server), the push-message decoder, and the typed Sender that
// builds and parses request envelopes.
package network

import (
	"context"
)

// Network is the transport abstraction. All communication between the SDK and
// the server (in-process LocalNetwork or remote HTTPNetwork) flows through
// this interface as JSON strings on the wire.
//
// Implementations must be safe for concurrent calls to Send and Recv.
type Network interface {
	PID() string
	Group() string
	Unicast() string
	Anycast() string

	// Start begins any background work (ticker, SSE listener). The supplied
	// context governs the lifetime of those goroutines.
	Start(ctx context.Context) error
	// Stop tears down background work and clears subscribers.
	Stop() error

	// Send issues a request envelope as a JSON string and returns the response
	// envelope JSON.
	Send(ctx context.Context, req string) (string, error)
	// Recv registers a callback for push-messages from the server. Callbacks
	// receive the raw JSON frame; the Sender layer parses it into a Message.
	Recv(cb func(raw string))

	// TargetResolver converts a logical target name to a transport-specific
	// address. For LocalNetwork this is local://any@<target>; for HTTPNetwork
	// it is poll://any@<target>.
	TargetResolver(target string) string
}
