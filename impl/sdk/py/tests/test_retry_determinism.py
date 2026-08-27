"""The retry *loop*, asserted on its delays rather than its wall-clock effect.

:mod:`tests.test_retry` covers the policies as pure functions. This module
covers the thing that consults them -- :meth:`Context.invoke_with_retry` -- by
injecting a :class:`~resonate.testing.RecordingSleeper` and asserting the exact
attempt and delay sequence.

That distinction matters. Counting calls (which the suite already did) proves
a body ran three times; it does not prove the policy was asked for the right
attempt numbers, that the returned delay was honoured, or that a workflow's
first failure short-circuits the loop before any delay is requested. Each of
those is a real bug the old tests would have passed straight through.
"""

from __future__ import annotations
