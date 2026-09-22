"""The two errors that cross the connector seam.

Small on purpose. Base owns :class:`ResonateError` (the root every failure
shares) and :class:`ConnectorError` (the only one a connector raises).
Everything else is durable-execution vocabulary and is tested in the SDK's own
suite.
"""

from __future__ import annotations

import pickle

from resonate_base.error import ConnectorError, ResonateError


def test_everything_here_derives_from_the_root() -> None:
    """The SDK's outermost catch is ``except ResonateError``.

    A connector error that missed this would kill the worker instead of
    releasing the task.
    """
    assert issubclass(ConnectorError, ResonateError)


def test_connector_error_wraps_and_exposes_its_cause() -> None:
    cause = TimeoutError("no responders")
    err = ConnectorError(cause)
    assert err.error is cause
    assert str(err) == "connector error: no responders"


def test_a_subclass_only_has_to_name_itself() -> None:
    """The name is the entire extension point.

    A connector that had to override ``__init__`` would eventually forget to
    forward to ``super().__init__``, breaking pickle arity and silently
    degrading every rejection it caused into a bare ``ApplicationError``.
    Inheriting the constructor makes that unreachable.
    """

    class SubstrateError(ConnectorError): ...

    err = SubstrateError(ValueError("unreachable"))
    assert str(err) == "connector error: unreachable"
    assert isinstance(err, ConnectorError)
    # ``args`` carries exactly the constructor's parameters, which is what
    # ``pickle``'s ``cls(*self.args)`` reconstruction depends on. Asserted
    # directly because a class defined inside a function cannot be pickled.
    assert err.args == (err.error,)
    assert type(err)(*err.args).error is err.error


def test_connector_error_survives_a_pickle_round_trip() -> None:
    """Rejections cross the durable boundary as pickles; this one must survive."""
    err = ConnectorError(ValueError("net down"))
    revived = pickle.loads(pickle.dumps(err))  # noqa: S301
    assert type(revived) is ConnectorError
    assert str(revived) == str(err)
