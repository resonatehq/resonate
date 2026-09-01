//! The console's read model: the `ui.*` requests.
//!
//! A second, read-only vocabulary beside the worker protocol in [`types`]. The
//! worker protocol is written for one participant acting on one promise or one
//! task; a console asks different questions — "the newest executions, sorted,
//! with a total", "this whole tree in one answer" — and answering them by
//! composing `promise.search` costs a request storm and still cannot sort.
//!
//! Four rules hold this namespace together, and they are why it is a namespace
//! rather than parameters bolted onto the worker requests:
//!
//! 1. **Read-only.** No `ui.*` request mutates. The console's one write —
//!    cancel — is `promise.settle` with `rejected_canceled`, the real request.
//! 2. **One request per screen**, answerable in one query plus an optional
//!    count.
//! 3. **Same envelope.** Same `corrId`, same `head.resonate:debug_time`, same
//!    auth. `process` stays a pure function of its input.
//! 4. **Additive.** Delete every `ui.*` handler and the server still serves.
//!
//! # What lives here rather than in an engine
//!
//! Everything that must be identical across backends. A request resolves into
//! a [`ExecutionsQuery`] / [`SchedulesQuery`] — limits clamped, sort parsed,
//! cursor decoded, prefix turned into a range — and a page of rows is finished
//! into a response by [`finish_executions_page`] /
//! [`finish_schedules_page`]. An engine writes the one SQL statement its
//! dialect needs and nothing else, so the four implementations cannot drift on
//! the parts that are easy to get subtly wrong.
//!
//! # Roots
//!
//! An *execution* is a root promise. Root-ness is not a tag: an id is
//! `<origin>:<lineage>`, so a root is exactly a promise whose id carries no
//! `':'` — which every backend already stores as the generated column
//! `origin_id`, and which `id = origin_id` tests without a scan of the tags.

use std::collections::HashMap;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use super::types::{PromiseRecord, PromiseState, PromiseValue, ResponseEnvelope, TaskState};

// ---------------------------------------------------------------------------
// Kinds
// ---------------------------------------------------------------------------

pub const EXECUTIONS_SEARCH: &str = "ui.executions.search";
pub const EXECUTION_GET: &str = "ui.execution.get";
pub const SCHEDULES_SEARCH: &str = "ui.schedules.search";

/// Every kind in this namespace, for a dispatcher that wants to enumerate them.
pub const KINDS: &[&str] = &[EXECUTIONS_SEARCH, EXECUTION_GET, SCHEDULES_SEARCH];

/// Is this kind part of the console vocabulary?
///
/// The prefix, not the list: a gateway that does not serve the console refuses
/// the whole namespace, including kinds a later build adds.
pub fn is_ui_kind(kind: &str) -> bool {
    kind.starts_with("ui.")
}

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

/// Page size when the client does not ask for one.
pub const DEFAULT_LIMIT: i64 = 50;
/// The largest page a client may ask for.
pub const MAX_LIMIT: i64 = 200;
/// Nodes returned by [`EXECUTION_GET`] when the client does not ask.
pub const DEFAULT_MAX_NODES: i64 = 500;
/// The largest tree a client may ask for. Beyond this the request is refused
/// rather than silently truncated to something smaller than it asked for.
pub const MAX_MAX_NODES: i64 = 5_000;

/// How many rows a `func`-filtered search reads to fill one page.
///
/// The function name lives inside the promise's base64 `param`, so it is not a
/// SQL predicate (see [`func_of`]). Filtering therefore happens over a bounded
/// window of the keyset scan, and the cursor advances to the last row
/// *examined* rather than the last row returned — so a page may come back
/// short, but no row is ever skipped or seen twice.
pub const FUNC_SCAN_LIMIT: i64 = 2_000;

/// The sort key of a row that has not settled: it settles at the end of time.
///
/// Sorting by `settledAt` has to place unsettled rows somewhere, and the three
/// dialects disagree about where NULLs go. Substituting a sentinel makes the
/// key total and the answer identical everywhere: unsettled sorts last
/// ascending, first descending.
pub const UNSETTLED_KEY: i64 = i64::MAX;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why a `ui.*` request was refused.
///
/// Rendered as a normal envelope with the status in `head`, and a `data` of
/// `{ error, message }` — a code the client can branch on, and a string a
/// person can read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UiError {
    /// A filter, sort, limit or cursor the request cannot have.
    InvalidRequest(String),
    /// The cursor was minted under a different sort. Pagination cannot
    /// continue; the client restarts from page one.
    CursorSortMismatch,
    /// No such execution.
    NotFound(String),
}

impl UiError {
    pub fn status(&self) -> i32 {
        match self {
            UiError::InvalidRequest(_) | UiError::CursorSortMismatch => 400,
            UiError::NotFound(_) => 404,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            UiError::InvalidRequest(_) => "invalid_request",
            UiError::CursorSortMismatch => "cursor_sort_mismatch",
            UiError::NotFound(_) => "not_found",
        }
    }

    pub fn message(&self) -> String {
        match self {
            UiError::InvalidRequest(m) => m.clone(),
            UiError::CursorSortMismatch => {
                "cursor was issued for a different sort; restart from the first page".to_string()
            }
            UiError::NotFound(id) => format!("no execution '{id}'"),
        }
    }

    pub fn to_response(&self, kind: String, corr_id: String) -> ResponseEnvelope {
        ResponseEnvelope::new(
            kind,
            corr_id,
            self.status(),
            serde_json::json!({ "error": self.code(), "message": self.message() }),
        )
    }
}

impl std::fmt::Display for UiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code(), self.message())
    }
}

fn invalid(message: impl Into<String>) -> UiError {
    UiError::InvalidRequest(message.into())
}

// ---------------------------------------------------------------------------
// Sorting
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    Asc,
    Desc,
}

impl Dir {
    /// `ORDER BY <expr> <this>`.
    pub fn sql(&self) -> &'static str {
        match self {
            Dir::Asc => "ASC",
            Dir::Desc => "DESC",
        }
    }

    /// The comparison a keyset cursor makes: strictly past the last row, in
    /// whichever direction the scan runs.
    pub fn cmp_sql(&self) -> &'static str {
        match self {
            Dir::Asc => ">",
            Dir::Desc => "<",
        }
    }

    fn parse(s: &str) -> Option<Dir> {
        match s {
            "asc" => Some(Dir::Asc),
            "desc" => Some(Dir::Desc),
            _ => None,
        }
    }
}

/// What an execution list is ordered by.
///
/// Every key is an integer and every key is total — `settledAt` through
/// [`UNSETTLED_KEY`] — which is what lets one keyset cursor shape serve all
/// three and all four backends produce the same page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionSortKey {
    CreatedAt,
    SettledAt,
    TimeoutAt,
}

impl ExecutionSortKey {
    fn name(&self) -> &'static str {
        match self {
            ExecutionSortKey::CreatedAt => "createdAt",
            ExecutionSortKey::SettledAt => "settledAt",
            ExecutionSortKey::TimeoutAt => "timeoutAt",
        }
    }

    /// The SQL expression to order by. ANSI in all three dialects.
    pub fn expr(&self) -> &'static str {
        match self {
            ExecutionSortKey::CreatedAt => "created_at",
            // Keep in step with UNSETTLED_KEY.
            ExecutionSortKey::SettledAt => "COALESCE(settled_at, 9223372036854775807)",
            ExecutionSortKey::TimeoutAt => "timeout_at",
        }
    }

    /// This row's position under this key.
    pub fn key_of(&self, p: &PromiseRecord) -> i64 {
        match self {
            ExecutionSortKey::CreatedAt => p.created_at,
            ExecutionSortKey::SettledAt => p.settled_at.unwrap_or(UNSETTLED_KEY),
            ExecutionSortKey::TimeoutAt => p.timeout_at,
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "createdAt" => Some(ExecutionSortKey::CreatedAt),
            "settledAt" => Some(ExecutionSortKey::SettledAt),
            "timeoutAt" => Some(ExecutionSortKey::TimeoutAt),
            _ => None,
        }
    }
}

/// What a schedule list is ordered by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleSortKey {
    NextRunAt,
    LastRunAt,
    CreatedAt,
}

impl ScheduleSortKey {
    fn name(&self) -> &'static str {
        match self {
            ScheduleSortKey::NextRunAt => "nextRunAt",
            ScheduleSortKey::LastRunAt => "lastRunAt",
            ScheduleSortKey::CreatedAt => "createdAt",
        }
    }

    pub fn expr(&self) -> &'static str {
        match self {
            ScheduleSortKey::NextRunAt => "next_run_at",
            // A schedule that has never run sorts as if it ran at the end of
            // time — the same rule an unsettled promise gets.
            ScheduleSortKey::LastRunAt => "COALESCE(last_run_at, 9223372036854775807)",
            ScheduleSortKey::CreatedAt => "created_at",
        }
    }

    pub fn key_of(&self, s: &super::types::ScheduleRecord) -> i64 {
        match self {
            ScheduleSortKey::NextRunAt => s.next_run_at,
            ScheduleSortKey::LastRunAt => s.last_run_at.unwrap_or(UNSETTLED_KEY),
            ScheduleSortKey::CreatedAt => s.created_at,
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "nextRunAt" => Some(ScheduleSortKey::NextRunAt),
            "lastRunAt" => Some(ScheduleSortKey::LastRunAt),
            "createdAt" => Some(ScheduleSortKey::CreatedAt),
            _ => None,
        }
    }
}

/// A sort as the wire spells it: `key:direction`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sort<K> {
    pub key: K,
    pub dir: Dir,
}

impl<K: Copy> Sort<K> {
    fn split(spec: &str) -> Result<(&str, Dir), UiError> {
        let (key, dir) = spec.split_once(':').ok_or_else(|| {
            invalid(format!(
                "invalid sort '{spec}' — expected '<key>:<asc|desc>'"
            ))
        })?;
        let dir = Dir::parse(dir)
            .ok_or_else(|| invalid(format!("invalid sort direction '{dir}' — asc or desc")))?;
        Ok((key, dir))
    }
}

impl Sort<ExecutionSortKey> {
    pub fn parse(spec: &str) -> Result<Self, UiError> {
        let (key, dir) = Self::split(spec)?;
        let key = ExecutionSortKey::parse(key).ok_or_else(|| {
            invalid(format!(
                "invalid sort key '{key}' — createdAt, settledAt or timeoutAt"
            ))
        })?;
        Ok(Sort { key, dir })
    }

    /// The spelling a cursor is stamped with, so a client that changes sort
    /// mid-pagination is caught rather than silently served a mixed page.
    pub fn canonical(&self) -> String {
        format!(
            "{}:{}",
            self.key.name(),
            if self.dir == Dir::Asc { "asc" } else { "desc" }
        )
    }
}

impl Sort<ScheduleSortKey> {
    pub fn parse(spec: &str) -> Result<Self, UiError> {
        let (key, dir) = Self::split(spec)?;
        let key = ScheduleSortKey::parse(key).ok_or_else(|| {
            invalid(format!(
                "invalid sort key '{key}' — nextRunAt, lastRunAt or createdAt"
            ))
        })?;
        Ok(Sort { key, dir })
    }

    pub fn canonical(&self) -> String {
        format!(
            "{}:{}",
            self.key.name(),
            if self.dir == Dir::Asc { "asc" } else { "desc" }
        )
    }
}

// ---------------------------------------------------------------------------
// Cursors
// ---------------------------------------------------------------------------

/// Where the previous page stopped: the sort key of its last row, and that
/// row's id as the tiebreak.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Keyset {
    pub key: i64,
    pub id: String,
}

#[derive(Serialize, Deserialize)]
struct RawCursor {
    /// The sort this cursor was minted under.
    s: String,
    /// The sort key of the last row.
    k: i64,
    /// That row's id.
    i: String,
}

/// Mint a cursor. Opaque and keyset-based — never an offset, which would skip
/// rows whenever something is created while the operator is paging.
pub fn encode_cursor(sort: &str, key: i64, id: &str) -> String {
    let raw = RawCursor {
        s: sort.to_string(),
        k: key,
        i: id.to_string(),
    };
    BASE64.encode(serde_json::to_vec(&raw).expect("RawCursor always serializes"))
}

/// Read a cursor back, refusing one minted under a different sort.
pub fn decode_cursor(cursor: &str, sort: &str) -> Result<Keyset, UiError> {
    let bytes = BASE64
        .decode(cursor)
        .map_err(|_| invalid("invalid cursor — not base64"))?;
    let raw: RawCursor =
        serde_json::from_slice(&bytes).map_err(|_| invalid("invalid cursor — not a cursor"))?;
    if raw.s != sort {
        return Err(UiError::CursorSortMismatch);
    }
    Ok(Keyset {
        key: raw.k,
        id: raw.i,
    })
}

// ---------------------------------------------------------------------------
// Shared request pieces
// ---------------------------------------------------------------------------

/// `["pending","rejected"]` and `"pending"` both mean a state filter.
///
/// The console sends the array; a hand-written request or a link is likelier to
/// send the scalar, and refusing it buys nothing.
fn state_list<'de, D>(d: D) -> Result<Option<Vec<PromiseState>>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(PromiseState),
        Many(Vec<PromiseState>),
    }
    Ok(Option::<OneOrMany>::deserialize(d)?.map(|v| match v {
        OneOrMany::One(s) => vec![s],
        OneOrMany::Many(v) => v,
    }))
}

fn clamp_limit(limit: Option<i64>) -> Result<i64, UiError> {
    match limit {
        None => Ok(DEFAULT_LIMIT),
        Some(n) if n < 1 => Err(invalid("limit must be at least 1")),
        Some(n) if n > MAX_LIMIT => Err(invalid(format!("limit must be at most {MAX_LIMIT}"))),
        Some(n) => Ok(n),
    }
}

/// The half-open id range that holds exactly the ids starting with `prefix`.
///
/// `[prefix, succ(prefix))`, where `succ` bumps the last character — which is
/// an index range rather than a `LIKE`, and cannot be confused by a `%` or `_`
/// in the prefix itself. `None` for the upper bound means the range is
/// open-ended, which only happens for a prefix ending at the last code point.
pub fn prefix_range(prefix: &str) -> (String, Option<String>) {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        // Skip the surrogate gap; `char::from_u32` returns None inside it.
        if let Some(next) = (last as u32 + 1..=0x10FFFF).find_map(char::from_u32) {
            let mut upper: String = chars.into_iter().collect();
            upper.push(next);
            return (prefix.to_string(), Some(upper));
        }
    }
    (prefix.to_string(), None)
}

// ---------------------------------------------------------------------------
// ui.executions.search
// ---------------------------------------------------------------------------

/// The request, as it arrives.
#[derive(Debug, Default, Deserialize)]
pub struct ExecutionsSearchData {
    #[serde(default, deserialize_with = "state_list")]
    pub state: Option<Vec<PromiseState>>,
    #[serde(default)]
    pub func: Option<String>,
    #[serde(rename = "idPrefix", default)]
    pub id_prefix: Option<String>,
    #[serde(default)]
    pub tags: Option<HashMap<String, String>>,
    #[serde(rename = "createdFrom", default)]
    pub created_from: Option<i64>,
    #[serde(rename = "createdTo", default)]
    pub created_to: Option<i64>,
    #[serde(default)]
    pub sort: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(rename = "countTotal", default)]
    pub count_total: bool,
}

/// The request, resolved: everything an engine needs and nothing it has to
/// decide for itself.
#[derive(Debug, Clone)]
pub struct ExecutionsQuery {
    /// Empty means every state.
    pub states: Vec<PromiseState>,
    pub func: Option<String>,
    /// Inclusive lower bound on the id, when `idPrefix` was given.
    pub id_from: Option<String>,
    /// Exclusive upper bound on the id.
    pub id_to: Option<String>,
    /// The tag filter, already JSON, in the form the engines' tag predicates
    /// take (`@>`, `JSON_CONTAINS`, `json_each` minus).
    pub tags_json: Option<String>,
    pub created_from: Option<i64>,
    pub created_to: Option<i64>,
    pub sort: Sort<ExecutionSortKey>,
    /// Rows the client asked for.
    pub limit: i64,
    /// Rows the engine should ask the database for. Larger than `limit` only
    /// when a `func` filter has to be applied after decoding the param.
    pub fetch: i64,
    pub after: Option<Keyset>,
    /// Whether to run the second, counting query. Never set when `func` is
    /// filtering, because that filter is not expressible in SQL and a count
    /// that ignored it would be a lie.
    pub count_total: bool,
}

impl ExecutionsSearchData {
    pub fn resolve(self) -> Result<ExecutionsQuery, UiError> {
        let sort = match &self.sort {
            Some(spec) => Sort::<ExecutionSortKey>::parse(spec)?,
            None => Sort {
                key: ExecutionSortKey::CreatedAt,
                dir: Dir::Desc,
            },
        };
        let limit = clamp_limit(self.limit)?;
        let after = match &self.cursor {
            Some(c) => Some(decode_cursor(c, &sort.canonical())?),
            None => None,
        };
        if let (Some(from), Some(to)) = (self.created_from, self.created_to) {
            if from > to {
                return Err(invalid("createdFrom must not be after createdTo"));
            }
        }
        let (id_from, id_to) = match self.id_prefix.as_deref() {
            Some("") | None => (None, None),
            Some(p) => {
                let (lo, hi) = prefix_range(p);
                (Some(lo), hi)
            }
        };
        let func = self.func.filter(|f| !f.is_empty());
        let fetch = if func.is_some() {
            FUNC_SCAN_LIMIT.max(limit)
        } else {
            limit
        };
        Ok(ExecutionsQuery {
            states: self.state.unwrap_or_default(),
            count_total: self.count_total && func.is_none(),
            func,
            id_from,
            id_to,
            tags_json: self
                .tags
                .filter(|t| !t.is_empty())
                .map(|t| serde_json::to_string(&t).expect("a string map always serializes")),
            created_from: self.created_from,
            created_to: self.created_to,
            sort,
            limit,
            fetch,
            after,
        })
    }
}

impl ExecutionsQuery {
    /// `AND state IN (…)`, or nothing.
    ///
    /// Inlined rather than bound: the values come from [`PromiseState`], a
    /// closed enum, so there is no caller text in the statement. Binding a
    /// list of unknown length is three different things in three drivers; this
    /// is one.
    pub fn states_sql(&self) -> String {
        if self.states.is_empty() {
            return String::new();
        }
        let list = self
            .states
            .iter()
            .map(|s| format!("'{}'", s.as_str()))
            .collect::<Vec<_>>()
            .join(", ");
        format!(" AND state IN ({list})")
    }
}

/// One row of the executions list.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionItem {
    pub id: String,
    pub state: PromiseState,
    /// The invoked function, when the param carries one. See [`func_of`].
    pub func: Option<String>,
    pub tags: HashMap<String, String>,
    #[serde(rename = "createdAt")]
    pub created_at: i64,
    #[serde(rename = "settledAt")]
    pub settled_at: Option<i64>,
    #[serde(rename = "timeoutAt")]
    pub timeout_at: i64,
}

/// Every list response in this namespace.
#[derive(Debug, Serialize)]
pub struct ListResponse<T> {
    pub items: Vec<T>,
    /// Absent when there is no next page. Present is the *only* signal that
    /// there is one — a full page is not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Only when `countTotal` was asked for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<i64>,
}

/// Turn a page of promise rows into the response.
///
/// `rows` is what the engine read: at most `fetch + 1` rows in sort order,
/// starting after the cursor. The extra row is how "there is more" is known
/// without a second query.
///
/// The `func` filter is applied here, after decoding each param — which is why
/// a filtered page can come back shorter than `limit` while still carrying a
/// cursor. The cursor then names the last row *examined*, so the next page
/// resumes exactly where this scan stopped.
pub fn finish_executions_page(
    q: &ExecutionsQuery,
    mut rows: Vec<PromiseRecord>,
    total: Option<i64>,
) -> ListResponse<ExecutionItem> {
    let scanned_more = rows.len() as i64 > q.fetch;
    rows.truncate(q.fetch as usize);
    let last_scanned = rows.last().map(|p| (q.sort.key.key_of(p), p.id.clone()));

    let mut items: Vec<ExecutionItem> = rows
        .into_iter()
        .filter(|p| match &q.func {
            None => true,
            Some(want) => func_of(&p.param).as_deref() == Some(want.as_str()),
        })
        .map(|p| ExecutionItem {
            func: func_of(&p.param),
            id: p.id,
            state: p.state,
            tags: p.tags,
            created_at: p.created_at,
            settled_at: p.settled_at,
            timeout_at: p.timeout_at,
        })
        .collect();

    let sort = q.sort.canonical();
    let cursor = if items.len() as i64 > q.limit {
        // More matches than a page: stop at the last one returned.
        items.truncate(q.limit as usize);
        let last = items.last().expect("limit >= 1, so there is a last item");
        Some(encode_cursor(
            &sort,
            match q.sort.key {
                ExecutionSortKey::CreatedAt => last.created_at,
                ExecutionSortKey::SettledAt => last.settled_at.unwrap_or(UNSETTLED_KEY),
                ExecutionSortKey::TimeoutAt => last.timeout_at,
            },
            &last.id,
        ))
    } else if scanned_more {
        // The page is not full but the scan was cut short: resume from where
        // it stopped, not from the last match.
        last_scanned
            .as_ref()
            .map(|(k, id)| encode_cursor(&sort, *k, id))
    } else {
        None
    };

    ListResponse {
        items,
        cursor,
        total,
    }
}

// ---------------------------------------------------------------------------
// ui.execution.get
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct ExecutionGetData {
    pub id: String,
    #[serde(rename = "maxNodes", default)]
    pub max_nodes: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ExecutionQuery {
    /// The root promise's id — the origin of whatever id was asked for, so a
    /// link to a step opens that step's execution.
    pub root_id: String,
    /// Rows to read. The engine asks for one more, to learn whether the tree
    /// was cut.
    pub max_nodes: i64,
}

impl ExecutionGetData {
    pub fn resolve(self) -> Result<ExecutionQuery, UiError> {
        if self.id.is_empty() {
            return Err(invalid("id is required"));
        }
        let max_nodes = match self.max_nodes {
            None => DEFAULT_MAX_NODES,
            Some(n) if n < 1 => return Err(invalid("maxNodes must be at least 1")),
            Some(n) if n > MAX_MAX_NODES => {
                return Err(invalid(format!("maxNodes must be at most {MAX_MAX_NODES}")))
            }
            Some(n) => n,
        };
        Ok(ExecutionQuery {
            root_id: origin_of(&self.id).to_string(),
            max_nodes,
        })
    }
}

/// The origin of an id: everything before the first `':'`.
///
/// The same split `resonate_core::types` applies and the same one every
/// backend stores as `origin_id`.
pub fn origin_of(id: &str) -> &str {
    id.split_once(':').map(|(o, _)| o).unwrap_or(id)
}

/// One promise in the tree.
///
/// Flat, with `parentId`, rather than nested: the client builds the tree, and a
/// flat array cannot be made cyclic by a malformed parent tag.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionNode {
    pub id: String,
    #[serde(rename = "parentId")]
    pub parent_id: Option<String>,
    pub state: PromiseState,
    pub func: Option<String>,
    pub param: PromiseValue,
    pub value: PromiseValue,
    pub tags: HashMap<String, String>,
    #[serde(rename = "createdAt")]
    pub created_at: i64,
    #[serde(rename = "settledAt")]
    pub settled_at: Option<i64>,
    #[serde(rename = "timeoutAt")]
    pub timeout_at: i64,
    /// The task this promise *is*, when it carries one — a promise runs as a
    /// task exactly when it names a dispatch target. `null` means it runs
    /// inside its parent's task, which is the fact the console's indentation
    /// rule needs and the one thing a client cannot reconstruct.
    #[serde(rename = "taskId")]
    pub task_id: Option<String>,
}

/// One task in the tree. [`TaskRecord`](super::types::TaskRecord) plus the two
/// timestamps the console draws with, which that record does not carry.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionTask {
    pub id: String,
    pub state: TaskState,
    pub version: i64,
    pub resumes: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: i64,
    /// When this task's current deadline falls: the lease while it is
    /// acquired, the retry deadline while it is pending, nothing otherwise.
    #[serde(rename = "expiresAt", skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct ExecutionResponse {
    pub root: PromiseRecord,
    /// Every promise in the execution, the root included, ordered by
    /// `createdAt` — which is also the console's display order.
    pub nodes: Vec<ExecutionNode>,
    pub tasks: Vec<ExecutionTask>,
    /// The tree was larger than `maxNodes`. An unbounded tree must not define
    /// a response size, and a client that is looking at half a tree has to be
    /// told.
    pub truncated: bool,
}

/// One promise row, as the engines read it for a tree.
///
/// The task columns come off the same row — a task *is* a promise with a
/// target — so this is what a single `SELECT` yields, not a join the engine
/// has to assemble.
#[derive(Debug, Clone)]
pub struct NodeRow {
    pub promise: PromiseRecord,
    pub task_state: Option<TaskState>,
    pub task_version: i64,
    pub resumes: i64,
    pub ttl: Option<i64>,
    pub pid: Option<String>,
    pub retry_timeout_at: Option<i64>,
    pub lease_timeout_at: Option<i64>,
}

/// Assemble the detail response from the rows of one execution.
///
/// Returns `NotFound` when the root is not among them — which is the same
/// answer as "no such execution", since every row in the set shares its origin.
pub fn build_execution(
    q: &ExecutionQuery,
    mut rows: Vec<NodeRow>,
) -> Result<ExecutionResponse, UiError> {
    let truncated = rows.len() as i64 > q.max_nodes;
    rows.truncate(q.max_nodes as usize);

    let root = rows
        .iter()
        .find(|r| r.promise.id == q.root_id)
        .map(|r| r.promise.clone())
        .ok_or_else(|| UiError::NotFound(q.root_id.clone()))?;

    let mut nodes = Vec::with_capacity(rows.len());
    let mut tasks = Vec::new();
    for row in rows {
        let is_task = row.task_state.is_some();
        if let Some(state) = row.task_state {
            tasks.push(ExecutionTask {
                id: row.promise.id.clone(),
                state,
                version: row.task_version,
                resumes: row.resumes,
                ttl: row.ttl,
                pid: row.pid,
                created_at: row.promise.created_at,
                expires_at: match state {
                    TaskState::Acquired => row.lease_timeout_at,
                    TaskState::Pending => row.retry_timeout_at,
                    _ => None,
                },
            });
        }
        let p = row.promise;
        nodes.push(ExecutionNode {
            parent_id: p.tags.get("resonate:parent").cloned(),
            task_id: is_task.then(|| p.id.clone()),
            func: func_of(&p.param),
            id: p.id,
            state: p.state,
            param: p.param,
            value: p.value,
            tags: p.tags,
            created_at: p.created_at,
            settled_at: p.settled_at,
            timeout_at: p.timeout_at,
        });
    }

    Ok(ExecutionResponse {
        root,
        nodes,
        tasks,
        truncated,
    })
}

// ---------------------------------------------------------------------------
// ui.schedules.search
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
pub struct SchedulesSearchData {
    #[serde(rename = "idPrefix", default)]
    pub id_prefix: Option<String>,
    #[serde(default)]
    pub tags: Option<HashMap<String, String>>,
    #[serde(default)]
    pub sort: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(rename = "countTotal", default)]
    pub count_total: bool,
}

#[derive(Debug, Clone)]
pub struct SchedulesQuery {
    pub id_from: Option<String>,
    pub id_to: Option<String>,
    pub tags_json: Option<String>,
    pub sort: Sort<ScheduleSortKey>,
    pub limit: i64,
    pub after: Option<Keyset>,
    pub count_total: bool,
}

impl SchedulesSearchData {
    pub fn resolve(self) -> Result<SchedulesQuery, UiError> {
        let sort = match &self.sort {
            Some(spec) => Sort::<ScheduleSortKey>::parse(spec)?,
            None => Sort {
                key: ScheduleSortKey::NextRunAt,
                dir: Dir::Asc,
            },
        };
        let limit = clamp_limit(self.limit)?;
        let after = match &self.cursor {
            Some(c) => Some(decode_cursor(c, &sort.canonical())?),
            None => None,
        };
        let (id_from, id_to) = match self.id_prefix.as_deref() {
            Some("") | None => (None, None),
            Some(p) => {
                let (lo, hi) = prefix_range(p);
                (Some(lo), hi)
            }
        };
        Ok(SchedulesQuery {
            id_from,
            id_to,
            tags_json: self
                .tags
                .filter(|t| !t.is_empty())
                .map(|t| serde_json::to_string(&t).expect("a string map always serializes")),
            sort,
            limit,
            after,
            count_total: self.count_total,
        })
    }
}

/// One row of the schedules list. The console shows five columns and this is
/// exactly those five plus the id.
#[derive(Debug, Clone, Serialize)]
pub struct ScheduleItem {
    pub id: String,
    pub cron: String,
    #[serde(rename = "promiseId")]
    pub promise_id: String,
    pub tags: HashMap<String, String>,
    #[serde(rename = "createdAt")]
    pub created_at: i64,
    #[serde(rename = "nextRunAt")]
    pub next_run_at: i64,
    #[serde(rename = "lastRunAt")]
    pub last_run_at: Option<i64>,
}

pub fn finish_schedules_page(
    q: &SchedulesQuery,
    mut rows: Vec<super::types::ScheduleRecord>,
    total: Option<i64>,
) -> ListResponse<ScheduleItem> {
    let has_more = rows.len() as i64 > q.limit;
    rows.truncate(q.limit as usize);
    let cursor = if has_more {
        rows.last()
            .map(|s| encode_cursor(&q.sort.canonical(), q.sort.key.key_of(s), &s.id))
    } else {
        None
    };
    ListResponse {
        items: rows
            .into_iter()
            .map(|s| ScheduleItem {
                id: s.id,
                cron: s.cron,
                promise_id: s.promise_id,
                tags: s.promise_tags,
                created_at: s.created_at,
                next_run_at: s.next_run_at,
                last_run_at: s.last_run_at,
            })
            .collect(),
        cursor,
        total,
    }
}

// ---------------------------------------------------------------------------
// The function name
// ---------------------------------------------------------------------------

/// The function a promise invokes, dug out of its param.
///
/// There is no column and no tag for this: the convention every SDK and the
/// CLI share is that `param.data` is a base64 JSON object with a `func` field
/// (see `resonate invoke`). So this is a decode, not a lookup — which is also
/// why `func` is not a SQL predicate and why filtering by it is a scan.
///
/// Tolerant on purpose: a param that is plain JSON rather than base64, or that
/// carries no `func`, is not an error — it is a promise nobody labelled.
pub fn func_of(param: &PromiseValue) -> Option<String> {
    let data = param.data.as_deref()?;
    let decoded = BASE64.decode(data).ok();
    let json: Value = match &decoded {
        Some(bytes) => serde_json::from_slice(bytes).ok()?,
        None => serde_json::from_str(data).ok()?,
    };
    json.get("func")?.as_str().map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn promise(id: &str, created: i64, settled: Option<i64>, func: Option<&str>) -> PromiseRecord {
        PromiseRecord {
            id: id.to_string(),
            state: if settled.is_some() {
                PromiseState::Resolved
            } else {
                PromiseState::Pending
            },
            param: PromiseValue {
                headers: None,
                data: func.map(|f| BASE64.encode(json!({ "func": f }).to_string())),
            },
            value: PromiseValue::default(),
            tags: HashMap::new(),
            timeout_at: created + 1000,
            created_at: created,
            settled_at: settled,
        }
    }

    fn query(limit: i64, func: Option<&str>) -> ExecutionsQuery {
        ExecutionsSearchData {
            limit: Some(limit),
            func: func.map(str::to_string),
            ..Default::default()
        }
        .resolve()
        .expect("valid")
    }

    #[test]
    fn the_default_sort_is_the_newest_first() {
        let q = query(10, None);
        assert_eq!(q.sort.canonical(), "createdAt:desc");
        assert_eq!(q.sort.key.expr(), "created_at");
        assert_eq!(q.sort.dir.sql(), "DESC");
        assert_eq!(q.sort.dir.cmp_sql(), "<");
    }

    #[test]
    fn an_unsettled_row_sorts_at_the_end_of_time() {
        // The three dialects disagree about where NULLs go, so there are no
        // NULLs: the sentinel is in the key and in the SQL expression alike.
        let p = promise("a", 5, None, None);
        assert_eq!(ExecutionSortKey::SettledAt.key_of(&p), UNSETTLED_KEY);
        assert!(ExecutionSortKey::SettledAt
            .expr()
            .contains(&UNSETTLED_KEY.to_string()));
    }

    #[test]
    fn a_cursor_round_trips_and_refuses_a_changed_sort() {
        let c = encode_cursor("createdAt:desc", 42, "checkout.order-1");
        let k = decode_cursor(&c, "createdAt:desc").expect("same sort");
        assert_eq!(k.key, 42);
        assert_eq!(k.id, "checkout.order-1");
        assert_eq!(
            decode_cursor(&c, "createdAt:asc").unwrap_err(),
            UiError::CursorSortMismatch
        );
        assert_eq!(UiError::CursorSortMismatch.status(), 400);
    }

    #[test]
    fn a_cursor_that_is_not_one_is_a_bad_request_not_a_panic() {
        assert_eq!(
            decode_cursor("not base64!!", "createdAt:desc").unwrap_err(),
            invalid("invalid cursor — not base64")
        );
        assert_eq!(
            decode_cursor(&BASE64.encode("{}"), "createdAt:desc").unwrap_err(),
            invalid("invalid cursor — not a cursor")
        );
    }

    #[test]
    fn a_full_page_carries_a_cursor_and_a_short_one_does_not() {
        let q = query(2, None);
        // Three rows for a page of two: the extra row is the "there is more".
        let rows = vec![
            promise("c", 30, None, None),
            promise("b", 20, None, None),
            promise("a", 10, None, None),
        ];
        let page = finish_executions_page(&q, rows, None);
        assert_eq!(page.items.len(), 2);
        let k = decode_cursor(page.cursor.as_deref().expect("more"), "createdAt:desc").unwrap();
        assert_eq!((k.key, k.id.as_str()), (20, "b"));

        let page = finish_executions_page(&q, vec![promise("a", 10, None, None)], None);
        assert!(page.cursor.is_none(), "one row is the whole answer");
    }

    #[test]
    fn a_func_filtered_page_resumes_from_the_last_row_scanned() {
        // The filter is applied after decoding the param, so a page can come
        // back short. What must not happen is a row being skipped: the cursor
        // names the last row examined, not the last row returned.
        let mut q = query(2, Some("charge"));
        q.fetch = 3;
        let rows = vec![
            promise("d", 40, None, Some("charge")),
            promise("c", 30, None, Some("refund")),
            promise("b", 20, None, Some("refund")),
            promise("a", 10, None, Some("charge")),
        ];
        let page = finish_executions_page(&q, rows, None);
        assert_eq!(page.items.len(), 1, "only one match in the scanned window");
        assert_eq!(page.items[0].id, "d");
        let k = decode_cursor(
            page.cursor.as_deref().expect("scan was cut"),
            "createdAt:desc",
        )
        .unwrap();
        assert_eq!(
            (k.key, k.id.as_str()),
            (20, "b"),
            "resumes at the last row scanned, so 'a' is not skipped"
        );
    }

    #[test]
    fn a_func_filter_turns_off_the_total() {
        // A count that ignored the filter would be a number for a different
        // question.
        let q = ExecutionsSearchData {
            func: Some("charge".to_string()),
            count_total: true,
            ..Default::default()
        }
        .resolve()
        .expect("valid");
        assert!(!q.count_total);
        assert_eq!(q.fetch, FUNC_SCAN_LIMIT);
    }

    #[test]
    fn limits_are_clamped_and_bad_ones_refused() {
        assert_eq!(query(10, None).limit, 10);
        assert_eq!(
            ExecutionsSearchData::default().resolve().unwrap().limit,
            DEFAULT_LIMIT
        );
        for bad in [0, -1, MAX_LIMIT + 1] {
            let e = ExecutionsSearchData {
                limit: Some(bad),
                ..Default::default()
            }
            .resolve()
            .unwrap_err();
            assert_eq!(e.status(), 400, "limit {bad}");
            assert_eq!(e.code(), "invalid_request");
        }
    }

    #[test]
    fn a_state_filter_is_one_value_or_many() {
        let one: ExecutionsSearchData =
            serde_json::from_value(json!({ "state": "pending" })).unwrap();
        assert_eq!(one.state.unwrap(), vec![PromiseState::Pending]);
        let many: ExecutionsSearchData =
            serde_json::from_value(json!({ "state": ["pending", "rejected"] })).unwrap();
        let q = ExecutionsSearchData {
            state: many.state,
            ..Default::default()
        }
        .resolve()
        .unwrap();
        assert_eq!(q.states_sql(), " AND state IN ('pending', 'rejected')");
        assert_eq!(query(1, None).states_sql(), "");
    }

    #[test]
    fn a_prefix_becomes_a_half_open_range() {
        // A range, not a LIKE: a '%' in the prefix is a character like any
        // other, and an index can serve it.
        let (lo, hi) = prefix_range("checkout.");
        assert_eq!(lo, "checkout.");
        assert_eq!(hi.as_deref(), Some("checkout/"));
        let (lo, hi) = prefix_range("a%");
        assert_eq!((lo.as_str(), hi.as_deref()), ("a%", Some("a&")));
    }

    #[test]
    fn an_execution_is_asked_for_by_any_id_within_it() {
        // A deep link to a step opens that step's execution.
        let q = ExecutionGetData {
            id: "checkout.order-8842:2.1".to_string(),
            max_nodes: None,
        }
        .resolve()
        .unwrap();
        assert_eq!(q.root_id, "checkout.order-8842");
        assert_eq!(q.max_nodes, DEFAULT_MAX_NODES);
    }

    #[test]
    fn an_oversized_tree_request_is_refused_rather_than_quietly_shrunk() {
        let e = ExecutionGetData {
            id: "x".to_string(),
            max_nodes: Some(MAX_MAX_NODES + 1),
        }
        .resolve()
        .unwrap_err();
        assert_eq!(e.code(), "invalid_request");
    }

    fn node_row(id: &str, parent: Option<&str>, task: Option<TaskState>) -> NodeRow {
        let mut p = promise(id, 1, None, None);
        if let Some(parent) = parent {
            p.tags
                .insert("resonate:parent".to_string(), parent.to_string());
        }
        NodeRow {
            promise: p,
            task_state: task,
            task_version: 1,
            resumes: 0,
            ttl: task.map(|_| 60),
            pid: task.map(|_| "wrk-1".to_string()),
            retry_timeout_at: Some(500),
            lease_timeout_at: Some(900),
        }
    }

    #[test]
    fn a_tree_names_its_tasks_and_keeps_the_root() {
        let q = ExecutionGetData {
            id: "o".to_string(),
            max_nodes: Some(10),
        }
        .resolve()
        .unwrap();
        let out = build_execution(
            &q,
            vec![
                node_row("o", None, Some(TaskState::Acquired)),
                node_row("o:1", Some("o"), None),
                node_row("o:2", Some("o"), Some(TaskState::Pending)),
            ],
        )
        .expect("root present");
        assert_eq!(out.root.id, "o");
        assert_eq!(out.nodes.len(), 3, "the root is a node too");
        assert_eq!(out.nodes[1].parent_id.as_deref(), Some("o"));
        assert_eq!(
            out.nodes[1].task_id, None,
            "a run child executes inside its parent's task"
        );
        assert_eq!(out.nodes[2].task_id.as_deref(), Some("o:2"));
        assert_eq!(out.tasks.len(), 2);
        // The deadline that is live is the one the state owns.
        assert_eq!(out.tasks[0].expires_at, Some(900));
        assert_eq!(out.tasks[1].expires_at, Some(500));
        assert!(!out.truncated);
    }

    #[test]
    fn a_tree_larger_than_asked_for_says_so() {
        let q = ExecutionGetData {
            id: "o".to_string(),
            max_nodes: Some(1),
        }
        .resolve()
        .unwrap();
        let out = build_execution(
            &q,
            vec![node_row("o", None, None), node_row("o:1", Some("o"), None)],
        )
        .unwrap();
        assert!(out.truncated);
        assert_eq!(out.nodes.len(), 1);
    }

    #[test]
    fn an_execution_whose_root_is_gone_is_not_found() {
        let q = ExecutionGetData {
            id: "o".to_string(),
            max_nodes: None,
        }
        .resolve()
        .unwrap();
        let e = build_execution(&q, vec![node_row("o:1", Some("o"), None)]).unwrap_err();
        assert_eq!(e.status(), 404);
        assert_eq!(e.code(), "not_found");
    }

    #[test]
    fn the_function_name_is_read_out_of_the_param() {
        let p = PromiseValue {
            headers: None,
            data: Some(BASE64.encode(r#"{"func":"processCheckout","args":[1]}"#)),
        };
        assert_eq!(func_of(&p).as_deref(), Some("processCheckout"));

        // Plain JSON, no base64 — tolerated.
        let p = PromiseValue {
            headers: None,
            data: Some(r#"{"func":"plain"}"#.to_string()),
        };
        assert_eq!(func_of(&p).as_deref(), Some("plain"));

        // Nothing to read is not an error.
        for data in [None, Some("not json".to_string()), Some("{}".to_string())] {
            assert_eq!(
                func_of(&PromiseValue {
                    headers: None,
                    data
                }),
                None
            );
        }
    }

    #[test]
    fn every_ui_kind_is_recognised_by_its_prefix() {
        for kind in KINDS {
            assert!(is_ui_kind(kind), "{kind}");
        }
        for kind in [
            "promise.get",
            "task.search",
            "schedule.create",
            "debug.snap",
        ] {
            assert!(!is_ui_kind(kind), "{kind}");
        }
    }

    #[test]
    fn an_error_renders_as_a_code_a_client_can_branch_on() {
        let resp = UiError::NotFound("o".to_string()).to_response("k".into(), "c1".into());
        assert_eq!(resp.head.status, 404);
        assert_eq!(resp.head.corr_id, "c1");
        assert_eq!(resp.data["error"], "not_found");
        assert!(resp.data["message"].as_str().unwrap().contains("'o'"));
    }
}
