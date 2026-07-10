"""pydantic shows opt-in Pydantic support at the durability boundary.

The codec serializes a ``pydantic.BaseModel`` (via ``model_dump``) and, when a
result is decoded against that model type, reconstructs the exact model. This
lets a durable workflow return a typed model that ``handle.result()`` hands
back fully validated.

Requires the ``pydantic`` extra::

    uv pip install -e '.[pydantic]'

Runs fully in-process (no server needed)::

    uv run python examples/pydantic

Reconstruction happens at every durability boundary: an internal ``ctx.run``
stage's declared return model is rebuilt on recovery just like the workflow's
own return is at the top-level handle.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import pydantic

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context


class LineItem(pydantic.BaseModel):
    sku: str
    qty: int = pydantic.Field(gt=0)
    unit_price: float = pydantic.Field(ge=0)


class Order(pydantic.BaseModel):
    id: str
    items: list[LineItem]
    total: float


class Pricing(pydantic.BaseModel):
    total: float


async def price_items(ctx: Context, items: list[dict]) -> Pricing:
    # A leaf stage: settles once, so side effects live here. Its declared
    # return model is rebuilt from the settled value on recovery.
    total = sum(i["qty"] * i["unit_price"] for i in items)
    print(f"  [price_items] {len(items)} items -> total={total}")
    return Pricing(total=total)


async def create_order(ctx: Context, order_id: str, items: list[dict]) -> Order:
    priced = await ctx.run(price_items, items)
    # The return annotation (Order) is what rebuilds the model for the awaiter.
    return Order(
        id=order_id,
        items=[LineItem(**i) for i in items],
        total=priced.total,
    )


async def main() -> None:
    r = Resonate()  # in-process LocalNetwork; no server required
    r.register(price_items)
    r.register(create_order)
    try:
        items = [
            {"sku": "A1", "qty": 2, "unit_price": 9.5},
            {"sku": "B2", "qty": 1, "unit_price": 4.0},
        ]
        id = f"pydantic-{time.time_ns()}"
        print(f"[create_order] starting workflow id={id}")
        handle = r.run(id, create_order, "ord-42", items)
        order = await handle.result()

        assert isinstance(order, Order)  # reconstructed to the model, not a dict
        assert order.total == 23.0
        print(f"[create_order] OK: {order!r}")
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
