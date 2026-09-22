package io.resonatehq.resonate;

import io.resonatehq.resonate.Errors.InvalidIdError;

/**
 * The promise id format, in one place.
 *
 * <p>The server treats a promise id as {@code <origin>:<lineage>}: the <b>origin</b> is everything
 * before the first {@code :}, and the lineage segments below it are {@code .}-separated:
 *
 * <pre>{@code root -> root:1 -> root:1.1 -> root:1.1.1}</pre>
 *
 * <p>The origin is load-bearing. {@code promise.register_callback} and {@code task.suspend} require
 * an awaiter and its awaited promise to share one, it selects the origin-state partition a request
 * is routed to, and {@code promise.create} rejects an id that does not extend the {@code
 * resonate:origin} / {@code resonate:branch} / {@code resonate:parent} it declares. So the SDK
 * mints ids with {@link #joinId} and reads them back with {@link #originOf}, both of which mirror
 * the server's own rules.
 *
 * <p>A root id is supplied by the caller and becomes the origin of its whole lineage, so {@link
 * #validateRootId} keeps {@code :} out of it, exactly as the server does for the origin tag itself.
 * {@code .} is <i>not</i> reserved there: it only separates segments below the origin, and the
 * origin is recovered by splitting on the first {@code :}, so a dotted root ({@code
 * my.app.workflow}) survives the round trip intact.
 */
public final class Ids {

    /** Separates the origin from the lineage below it. A bare root joins its first segment with this. */
    static final String ORIGIN_SEP = ":";

    /** Separates lineage segments below the origin. */
    static final String LINEAGE_SEP = ".";

    private Ids() {}

    /**
     * Append a lineage {@code segment} to {@code ancestor}.
     *
     * <p>A bare root joins its <i>first</i> segment with {@code :}; an ancestor that already carries
     * lineage joins deeper segments with {@code .}, keeping the whole subtree under one origin:
     *
     * <pre>{@code
     * joinId("root", "1")     -> "root:1"
     * joinId("root:1", "2")   -> "root:1.2"
     * joinId("root:1.2", "3") -> "root:1.2.3"
     * }</pre>
     *
     * <p>This is exactly the separator rule the server's {@code resonate:branch} / {@code
     * resonate:parent} validation applies.
     */
    public static String joinId(String ancestor, String segment) {
        String sep = ancestor.contains(ORIGIN_SEP) ? LINEAGE_SEP : ORIGIN_SEP;
        return ancestor + sep + segment;
    }

    /**
     * The lineage origin of {@code id}: everything before the first {@code :}.
     *
     * <p>Mirrors the server's {@code origin()}. An id with no lineage below it (a root) is its own
     * origin.
     */
    public static String originOf(String id) {
        int sep = id.indexOf(ORIGIN_SEP);
        return sep == -1 ? id : id.substring(0, sep);
    }

    /**
     * Validate a caller-supplied root id ({@code run} / {@code rpc} / {@code schedule}), returning it.
     *
     * <p>Only {@code :} is <b>reserved</b>: a root becomes the origin of its whole lineage, and the
     * origin is everything before an id's <i>first</i> {@code :}, so an origin holding one could never
     * be split back out of any id. The server rejects it outright ({@code colon_in_origin}).
     *
     * <p>{@code .} is allowed. It separates lineage segments <i>below</i> the origin, which is only
     * ever read after the origin has been split off, so a dotted root id ({@code my.app.workflow}) is
     * unambiguous:
     *
     * <pre>{@code my.app.workflow -> my.app.workflow:1 -> my.app.workflow:1.1}</pre>
     *
     * @throws InvalidIdError here, at the call site that named the workflow, rather than surfacing
     *     later as an opaque 400 from a background create.
     */
    public static String validateRootId(String id) {
        if (id == null || id.isEmpty()) {
            throw new InvalidIdError(id, "id must not be empty");
        }
        if (id.indexOf('\0') != -1) {
            throw new InvalidIdError(id, "id must not contain null bytes");
        }
        if (id.contains(ORIGIN_SEP)) {
            throw new InvalidIdError(
                    id,
                    "id must not contain '%s': it separates the origin from the lineage in the ids the SDK mints below this one, so an id holding one could never be split back out"
                            .formatted(ORIGIN_SEP));
        }
        return id;
    }
}
