//! The promise id format, in one place.
//!
//! The server treats a promise id as `<origin>:<lineage>`: the **origin** is
//! everything before the first `:`, and the lineage segments below it are
//! `.`-separated:
//!
//! ```text
//! root -> root:1 -> root:1.1 -> root:1.1.1
//! ```
//!
//! The origin is load-bearing. `promise.register_callback` and `task.suspend`
//! require an awaiter and its awaited promise to share one, it selects the
//! origin-state partition a request is routed to, and `promise.create` rejects
//! an id that does not extend the `resonate:origin` / `resonate:branch` /
//! `resonate:parent` it declares. So the SDK mints ids with [`join_id`] and
//! reads them back with [`origin_of`], both of which mirror the server's own
//! rules.
//!
//! A root id is supplied by the caller and becomes the origin of its whole
//! lineage, so [`validate_root_id`] keeps `:` out of it, exactly as the server
//! does for the origin tag itself. `.` is *not* reserved there: it only
//! separates segments below the origin, and the origin is recovered by
//! splitting on the first `:`, so a dotted root (`my.app.workflow`) survives
//! the round trip intact.

use crate::error::{Error, Result};

/// Separates the origin from the lineage below it. A bare root joins its
/// first lineage segment with this.
pub const ORIGIN_SEP: char = ':';

/// Separates lineage segments below the origin.
pub const LINEAGE_SEP: char = '.';

/// Append a lineage `segment` to `ancestor`.
///
/// A bare root joins its *first* segment with `:`; an ancestor that already
/// carries lineage joins deeper segments with `.`, keeping the whole subtree
/// under one origin:
///
/// ```text
/// join_id("root", "1")     -> "root:1"
/// join_id("root:1", "2")   -> "root:1.2"
/// join_id("root:1.2", "3") -> "root:1.2.3"
/// ```
///
/// This is exactly the separator rule the server's `resonate:branch` /
/// `resonate:parent` validation applies.
pub fn join_id(ancestor: &str, segment: &str) -> String {
    let sep = if ancestor.contains(ORIGIN_SEP) {
        LINEAGE_SEP
    } else {
        ORIGIN_SEP
    };
    format!("{}{}{}", ancestor, sep, segment)
}

/// Return the lineage origin of `id`: everything before the first `:`.
///
/// Mirrors the server's `origin()`. An id with no lineage below it (a root)
/// is its own origin.
pub fn origin_of(id: &str) -> &str {
    id.split(ORIGIN_SEP).next().unwrap_or(id)
}

/// Validate a caller-supplied root id (`run` / `rpc` / `schedule`).
///
/// Only `:` is **reserved**: a root becomes the origin of its whole lineage,
/// the origin is everything before an id's *first* `:`, so an origin holding
/// one could never be split back out of any id. The server rejects it outright
/// (`colon_in_origin`).
///
/// `.` is allowed. It separates lineage segments *below* the origin, which is
/// only ever read after the origin has been split off, so a dotted root id
/// (`my.app.workflow`) is unambiguous:
///
/// ```text
/// my.app.workflow -> my.app.workflow:1 -> my.app.workflow:1.1
/// ```
///
/// Returns [`Error::InvalidId`] here, at the call site that named the
/// workflow, rather than surfacing later as an opaque 400 from the server.
pub fn validate_root_id(id: &str) -> Result<()> {
    if id.is_empty() {
        return Err(Error::InvalidId {
            id: id.to_string(),
            reason: "id must not be empty".to_string(),
        });
    }
    if id.contains('\0') {
        return Err(Error::InvalidId {
            id: id.to_string(),
            reason: "id must not contain null bytes".to_string(),
        });
    }
    if id.contains(ORIGIN_SEP) {
        return Err(Error::InvalidId {
            id: id.to_string(),
            reason: format!(
                "id must not contain {:?}: it separates the origin from the \
                 lineage in the ids the SDK mints below this one, so an id \
                 holding one could never be split back out",
                ORIGIN_SEP
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_id_matches_the_servers_separator_rule() {
        assert_eq!(join_id("root", "1"), "root:1");
        assert_eq!(join_id("root:1", "2"), "root:1.2");
        assert_eq!(join_id("root:1.2", "3"), "root:1.2.3");
        assert_eq!(join_id("root", "dbeef"), "root:dbeef");
    }

    #[test]
    fn origin_of_is_everything_before_the_first_colon() {
        assert_eq!(origin_of("root"), "root");
        assert_eq!(origin_of("root:1"), "root");
        assert_eq!(origin_of("root:1.2"), "root");
        assert_eq!(origin_of("root:dbeef"), "root");
    }

    #[test]
    fn validate_root_id_rejects_reserved_separators() {
        // ':' is the one reserved separator in a root id: it becomes the
        // origin of its whole lineage, and the server rejects an origin
        // containing one outright (colon_in_origin).
        for id in ["a:b", "a.b:c", "", "a\0b"] {
            assert!(
                matches!(validate_root_id(id), Err(Error::InvalidId { .. })),
                "expected InvalidId for {:?}",
                id
            );
        }
    }

    #[test]
    fn a_dot_in_a_root_id_is_accepted() {
        // '.' only separates lineage segments *below* the origin, which is
        // read after the origin has been split off at the first ':'. A dotted
        // root is therefore unambiguous, and the server takes it.
        assert!(validate_root_id("my.app.workflow").is_ok());
        let id = join_id("my.app.workflow", "1");
        assert_eq!(id, "my.app.workflow:1");
        assert_eq!(origin_of(&id), "my.app.workflow");
        let tags = serde_json::json!({ "resonate:origin": "my.app.workflow" });
        crate::test_utils::server_validate(&id, &tags);
    }

    #[test]
    fn validate_root_id_accepts_bare_ids() {
        for id in ["a", "a-b", "a_b", "a.b", "wf-1786636678653183000"] {
            assert!(validate_root_id(id).is_ok(), "rejected {:?}", id);
        }
    }
}
