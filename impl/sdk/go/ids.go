package resonate

import (
	"fmt"
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
// lineage, so ValidateRootID keeps both separators out of it, exactly as the
// server does for the origin tag itself.

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
// Both separators are **reserved**: a root becomes the origin of its whole
// lineage, and the server rejects an origin containing either one outright
// (dot_in_origin / colon_in_origin). "." because it separates lineage
// segments; ":" because the origin is everything before an id's *first* ":",
// so an origin holding one could never be split back out of any id.
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
	for _, sep := range []string{lineageSep, originSep} {
		if strings.Contains(id, sep) {
			return &InvalidIDError{
				ID: id,
				Reason: fmt.Sprintf("id must not contain %q: it is reserved as a lineage"+
					" separator in the ids the SDK mints below this one", sep),
			}
		}
	}
	return nil
}
