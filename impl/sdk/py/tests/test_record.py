from __future__ import annotations

from typing import Any

from resonate.actions import LFI, RFI
from resonate.context import Context
from resonate.dataclasses import Invocation
from resonate.dependencies import Dependencies
from resonate.record import Record


def test_lineage() -> None:
    lfi = LFI(Invocation("foo", 1, 2))
    rfi = RFI(Invocation("foo", 1, 2))
    n0 = Record[Any](id="n0", invocation=lfi, parent=None, ctx=Context(Dependencies()))
    assert n0.is_root
    assert n0.root() == n0
    assert n0.children == []
    n1 = n0.create_child("n1", rfi)
    assert n0.children == [n1]
    n2 = n0.create_child("n2", rfi)
    assert n0.children == [n1, n2]
    assert n1.root() == n1
    assert n2.root() == n2
    assert n1.children == []
    assert n2.children == []

    n3 = n1.create_child("n3", lfi)
    n4 = n1.create_child("n4", lfi)
    n5 = n2.create_child("n5", lfi)
    n6 = n2.create_child("n6", lfi)
    assert not n3.is_root
    assert not n4.is_root
    assert not n5.is_root
    assert not n6.is_root
    assert n3.root() == n1
    assert n4.root() == n1
    assert n5.root() == n2
    assert n6.root() == n2

    assert n1.children == [n3, n4]
    assert n2.children == [n5, n6]
    assert n0.children == [n1, n2]


def test_lineage_2() -> None:
    lfi = LFI(Invocation("foo", 1, 2))
    rfi = RFI(Invocation("foo", 1, 2))
    foo = Record[Any](
        id="foo", invocation=rfi, parent=None, ctx=Context(Dependencies())
    )
    bar = foo.create_child(id="bar", invocation=lfi)
    bar.create_child(id="baz", invocation=rfi)
    assert foo.children == [bar]
