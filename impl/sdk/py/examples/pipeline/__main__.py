"""pipeline shows a multi-stage DAG-shaped durable workflow:

    download -> parse -> +- transform_a -+
                         +- transform_b -+- merge -> emit

transform_a and transform_b run in parallel (both RPCs are dispatched before
either is awaited); merge depends on both and synchronizes them with await.
Every stage is a registered function backed by a durable promise, so a crash
mid-pipeline picks up at the first unsettled stage without re-doing completed
work.

Start a Resonate server on localhost:8001 first (``resonate dev``), then::

    uv run python examples/pipeline

Note on replay: a durable orchestrator re-executes from the top each time it
awaits a not-yet-settled future, so any side effect (a ``print``, an external
call) belongs in a leaf stage function -- which settles once and never re-runs
-- not in ``run_pipeline`` itself. That is why every log line below lives in a
stage, never in the orchestrator.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import TYPE_CHECKING

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context


async def download(ctx: Context, url: str) -> str:
    body = f"the quick brown fox jumps over {url}"
    print(f"  [download] {url} -> {len(body)} bytes")
    return body


async def parse(ctx: Context, raw: str) -> list[str]:
    words = raw.split()
    print(f"  [parse] {len(words)} words")
    return words


async def transform_a(ctx: Context, p: list[str]) -> int:
    print("  [transform_a] counting words")
    return len(p)


async def transform_b(ctx: Context, p: list[str]) -> str:
    upper = " ".join(p).upper()
    print(f"  [transform_b] uppercased {len(upper)} chars")
    return upper


async def merge(ctx: Context, word_count: int, upper: str) -> tuple[int, str]:
    print("  [merge] combining transforms")
    return word_count, upper


async def emit(ctx: Context, word_count: int, upper: str) -> str:
    print(f"  [emit] words={word_count} upper={upper!r}")
    return "ok"


async def run_pipeline(ctx: Context, url: str) -> str:
    raw = await ctx.run(download, url)
    parsed = await ctx.run(parse, raw)

    # fan out: transform_a and transform_b dispatched before either is awaited
    fa = ctx.run(transform_a, parsed)
    fb = ctx.run(transform_b, parsed)

    # fan in
    a = await fa
    b = await fb

    word_count, upper = await ctx.run(merge, word_count=a, upper=b)
    return await ctx.run(emit, word_count, upper)


async def main() -> None:
    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)
    r.register(run_pipeline)

    try:
        id = f"pipeline-{time.time_ns()}"
        print(f"[run_pipeline] starting workflow id={id}")
        handle = r.run(id, run_pipeline, "example.com/doc")
        out = await handle.result()
        assert out == "ok"
        print(f"[run_pipeline] OK: sent={out!r}")
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
