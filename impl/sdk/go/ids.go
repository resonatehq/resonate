package resonate

import (
	"strings"
)

// The promise id format, in one place.
//
// The server treats a promise id as `<origin>:<lineage>`: the **origin** is
// everything before the first `:`, and the lineage segments below it are
// `.`-separated:
//
//	root -> root:1 -> root:1.1 -> root:1.1.1
//
// The origin is load-bearing. promise.registerCallback and task.suspend
// require an awaiter and its awaited promise to share one, it selects the
// origin-state partition a request is routed to, and promise.create rejects
// an id that does not extend the resonate:origin / resonate:branch /
// resonate:parent it declares. So the SDK mints ids with JoinID and reads
// them back with OriginOf, both of which mirror the server's own rules.
//
// A root id is supplied by the caller and becomes the origin of its whole
// lineage, so ValidateRootID keeps ":" out of it, exactly as the server does
// for the origin tag itself. "." is *not* reserved there: it only separates
// segments below the origin, and the origin is recovered by splitting on the
// first ":", so a dotted root (my.app.workflow) survives the round trip
// intact.

const (
	// originSep separates the origin from the lineage below it. A bare root
	// joins its first lineage segment with this.
	originSep = ":"

	// lineageSep separates lineage segments below the origin.
	lineageSep = "."
)

// JoinID appends a lineage segment to ancestor.
//
// A bare root joins its *first* segment with ":"; an ancestor that already
// carries lineage joins deeper segments with ".", keeping the whole subtree
// under one origin:
//
//	JoinID("root", "1")     -> "root:1"
//	JoinID("root:1", "2")   -> "root:1.2"
//	JoinID("root:1.2", "3") -> "root:1.2.3"
//
// This is exactly the separator rule the server's resonate:branch /
// resonate:parent validation applies.
func JoinID(ancestor, segment string) string {
	sep := originSep
	if strings.Contains(ancestor, originSep) {
		sep = lineageSep
	}
	return ancestor + sep + segment
}

// OriginOf returns the lineage origin of id: everything before the first ":".
//
// Mirrors the server's origin(). An id with no lineage below it (a root) is
// its own origin.
func OriginOf(id string) string {
	origin, _, _ := strings.Cut(id, originSep)
	return origin
}

// ValidateRootID checks a caller-supplied root id (Run / RPC / a schedule id)
// and returns *InvalidIDError when the server's id format cannot carry it.
//
// Only ":" is **reserved**: a root becomes the origin of its whole lineage,
// the origin is everything before an id's *first* ":", so an origin holding
// one could never be split back out of any id. The server rejects it outright
// (colon_in_origin).
//
// "." is allowed. It separates lineage segments *below* the origin, which is
// only ever read after the origin has been split off, so a dotted root id
// (my.app.workflow) is unambiguous:
//
//	my.app.workflow -> my.app.workflow:1 -> my.app.workflow:1.1
//
// The error is raised here, at the call site that named the workflow, rather
// than surfacing later as an opaque 400 from the server.
func ValidateRootID(id string) error {
	if id == "" {
		return &InvalidIDError{ID: id, Reason: "id must not be empty"}
	}
	if strings.ContainsRune(id, '\x00') {
		return &InvalidIDError{ID: id, Reason: "id must not contain null bytes"}
	}
	if strings.Contains(id, originSep) {
		return &InvalidIDError{
			ID: id,
			Reason: "id must not contain \":\": it separates the origin from " +
				"the lineage in the ids the SDK mints below this one, so an " +
				"id holding one could never be split back out",
		}
	}
	return nil
}
