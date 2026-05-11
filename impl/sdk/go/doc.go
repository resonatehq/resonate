// Package resonate is the Go SDK for Resonate's durable execution platform.
//
// This package owns the shared wire types, codec, and errors used by the
// transport layer. The transport seam (the [network.Network] interface, the
// in-process [network.LocalNetwork], the remote [network.HTTPNetwork], and the
// typed [network.Sender]) lives in the network subpackage.
package resonate
