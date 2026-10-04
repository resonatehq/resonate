"""The mutants: one deliberate bug each, as an exact edit of the source.

Every edit must match exactly once, so a mutant that no longer applies
(the code moved) fails loudly instead of silently testing the original.

`kind` says what the bug breaks: "safety" (a wrong answer the checkers can
refute) or "liveness" (work that never finishes — a workload sees it as runs
that time out, no checker can).
"""

MUTANTS = [
    {
        "name": "timer-targeted-accepted",
        "kind": "safety",
        "why": "today's bug: promise.create accepts a timer with a target",
        "file": "src/handle.zig",
        "old": """    if (protocol.timer_targeted(c.tags)) return ctx.fail(400, protocol.timer_targeted_message);
    if (c.tags.get(protocol.tag_target)) |addr| {
        if (!protocol.is_valid_address(addr)) return ctx.fail(400, "Invalid resonate:target address");
    }

    try try_timeout(ctx, &.{c.id});
    // Idempotent""",
        "new": """    if (c.tags.get(protocol.tag_target)) |addr| {
        if (!protocol.is_valid_address(addr)) return ctx.fail(400, "Invalid resonate:target address");
    }

    try try_timeout(ctx, &.{c.id});
    // Idempotent""",
    },
    {
        "name": "heartbeat-revives-dead-lease",
        "kind": "safety",
        "why": "the heartbeat fast path extends a lease whose promise is past its deadline",
        "file": "src/handle.zig",
        "old": "break :blk promise.state != .pending or promise.timeout_at > ctx.now;",
        "new": "_ = promise; break :blk true;",
    },
    {
        "name": "acquire-keeps-version",
        "kind": "safety",
        "why": "task.acquire does not bump the version, so a stale holder's fence still lands",
        "file": "src/handle.zig",
        "old": "    t.state = .acquired;\n    t.version = version + 1;",
        "new": "    t.state = .acquired;\n    t.version = version;",
    },
    {
        "name": "settle-overwrites",
        "kind": "safety",
        "why": "promise.settle on a settled promise replaces the first verdict",
        "file": "src/handle.zig",
        "old": """    // Settling a settled promise is idempotent and reports what it holds: the
    // first verdict stands, because something has already acted on it.
    if (promise.state != .pending) {""",
        "new": """    // Settling a settled promise is idempotent and reports what it holds: the
    // first verdict stands, because something has already acted on it.
    if (false) {""",
    },
    {
        "name": "blind-commit",
        "kind": "safety",
        "why": "the document is written without If-Match: a lost update between two servers",
        "file": "src/applier.zig",
        "old": ".precondition = if (self.etag) |e| .{ .match = e } else .absent,",
        "new": ".precondition = .none,",
    },
    {
        "name": "trusted-cache",
        "kind": "safety",
        "why": "a cached document is decided on without asking the bucket whether it is current",
        "file": "src/applier.zig",
        "old": "            if (!self.applier.cfg.linearizable_reads) {",
        "new": "            if (true) {",
    },
    {
        "name": "timer-rejects",
        "kind": "safety",
        "why": "a timer's deadline rejects it instead of resolving it",
        "file": "src/protocol.zig",
        "old": "return if (is_timer(tags)) .resolved else .rejected_timedout;",
        "new": "return .rejected_timedout;",
    },
    {
        "name": "fence-ignores-version",
        "kind": "safety",
        "why": "task.fence commits for any version of an acquired task",
        "file": "src/handle.zig",
        "old": 'if (t.state != .acquired or t.version != version) return ctx.fail(409, "Version mismatch");',
        "new": 'if (t.state != .acquired) return ctx.fail(409, "Version mismatch");',
    },
    {
        "name": "get-skips-deadline",
        "kind": "safety",
        "why": "promise.get answers pending for a promise past its deadline",
        "file": "src/handle.zig",
        "old": """    try try_timeout(ctx, &.{id});
    const promise = ctx.doc.promise(id) orelse return ctx.fail(404, "Promise not found");
    var w = ctx.writer();""",
        "new": """    const promise = ctx.doc.promise(id) orelse return ctx.fail(404, "Promise not found");
    var w = ctx.writer();""",
    },
    {
        "name": "suspend-ignores-settled",
        "kind": "liveness",
        "why": "task.suspend suspends on a promise that is already settled: a lost wakeup",
        "file": "src/handle.zig",
        "old": "    if (any_settled) {",
        "new": "    if (false) {",
    },
    {
        "name": "settlement-resumes-nobody",
        "kind": "liveness",
        "why": "settling a promise does not resume the tasks awaiting it",
        "file": "src/handle.zig",
        "old": "    try trigger_fulfilled(ctx, promise_id);\n    try trigger_callbacks(ctx, promise_id);",
        "new": "    try trigger_fulfilled(ctx, promise_id);",
    },
    {
        "name": "timer-unarmed",
        "kind": "liveness",
        "why": "deadlines are armed only for promises with a target again (before today's fix)",
        "file": "src/handle.zig",
        "old": "        .timeout_armed = !already_timedout and protocol.is_external(tags),",
        "new": "        .timeout_armed = !already_timedout and tags.has(protocol.tag_target),",
    },
]


def apply(root, mutant):
    path = f"{root}/{mutant['file']}"
    text = open(path).read()
    n = text.count(mutant["old"])
    if n != 1:
        raise SystemExit(f"mutant {mutant['name']}: the edit matches {n} times in {mutant['file']}, not once")
    open(path, "w").write(text.replace(mutant["old"], mutant["new"]))
