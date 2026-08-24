// The promise id format, in one place.
//
// The server treats a promise id as `<origin>:<lineage>`: the **origin** is
// everything before the first `:`, and the lineage segments below it are
// `.`-separated:
//
//   root -> root:1 -> root:1.1 -> root:1.1.1
//
// The origin is load-bearing. `promise.register_callback` and `task.suspend`
// require an awaiter and its awaited promise to share one, it selects the
// origin-state partition a request is routed to, and `promise.create` rejects
// an id that does not extend the `resonate:origin` / `resonate:branch` /
// `resonate:parent` it declares. So the SDK mints ids with `joinId` and reads
// them back with `originOf`, both of which mirror the server's own rules.
//
// A root id is supplied by the caller and becomes the origin of its whole
// lineage, so `validateRootId` keeps `:` out of it, exactly as the server does
// for the origin tag itself. `.` is *not* reserved there: it only separates
// segments below the origin, and the origin is recovered by splitting on the
// first `:`, so a dotted root (`my.app.workflow`) survives the round trip
// intact.

import exceptions from "./exceptions.js";

/** Separates the origin from the lineage below it. A bare root joins its
 * first lineage segment with this. */
export const ORIGIN_SEP = ":";

/** Separates lineage segments below the origin. */
export const LINEAGE_SEP = ".";

/**
 * Append a lineage `segment` to `ancestor`.
 *
 * A bare root joins its *first* segment with `:`; an ancestor that already
 * carries lineage joins deeper segments with `.`, keeping the whole subtree
 * under one origin:
 *
 *   joinId("root", "1")     -> "root:1"
 *   joinId("root:1", "2")   -> "root:1.2"
 *   joinId("root:1.2", "3") -> "root:1.2.3"
 *
 * This is exactly the separator rule the server's `resonate:branch` /
 * `resonate:parent` validation applies.
 */
export function joinId(ancestor: string, segment: string): string {
  const sep = ancestor.includes(ORIGIN_SEP) ? LINEAGE_SEP : ORIGIN_SEP;
  return `${ancestor}${sep}${segment}`;
}

/**
 * The lineage origin of `id`: everything before the first `:`.
 *
 * Mirrors the server's `origin()`. An id with no lineage below it (a root) is
 * its own origin.
 */
export function originOf(id: string): string {
  const sep = id.indexOf(ORIGIN_SEP);
  return sep === -1 ? id : id.slice(0, sep);
}

/**
 * Validate a caller-supplied root id (`run` / `rpc` / `schedule` / an explicit
 * `options({ id })`), returning it.
 *
 * Only `:` is **reserved**: a root becomes the origin of its whole lineage,
 * and the origin is everything before an id's *first* `:`, so an origin
 * holding one could never be split back out of any id. The server rejects it
 * outright (`colon_in_origin`).
 *
 * `.` is allowed. It separates lineage segments *below* the origin, which is
 * only ever read after the origin has been split off, so a dotted root id
 * (`my.app.workflow`) is unambiguous:
 *
 *   my.app.workflow -> my.app.workflow:1 -> my.app.workflow:1.1
 *
 * @throws {ResonateError} here, at the call site that named the workflow,
 *   rather than surfacing later as an opaque 400 from a background create.
 */
export function validateRootId(id: string): string {
  if (!id) {
    throw exceptions.INVALID_ID(id, "id must not be empty");
  }
  if (id.includes("\0")) {
    throw exceptions.INVALID_ID(id, "id must not contain null bytes");
  }
  if (id.includes(ORIGIN_SEP)) {
    throw exceptions.INVALID_ID(
      id,
      `id must not contain '${ORIGIN_SEP}': it separates the origin from the lineage in the ids the SDK mints below this one, so an id holding one could never be split back out`,
    );
  }
  return id;
}
