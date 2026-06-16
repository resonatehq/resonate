from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import msgspec

from resonate.error import DecodingError, ServerError
from resonate.types import PromiseRecord

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.network import Network

logger = logging.getLogger(__name__)


# =============================================================================
# Incoming messages (recv path)
# =============================================================================


class TaskRef(msgspec.Struct, kw_only=True, frozen=True):
    id: str
    version: int = msgspec.field(default=0)


class ExecuteData(msgspec.Struct, kw_only=True, frozen=True):
    task: TaskRef


class ExecuteMsg(
    msgspec.Struct, tag="execute", tag_field="kind", kw_only=True, frozen=True
):
    data: ExecuteData

    @property
    def task_id(self) -> str:
        return self.data.task.id

    @property
    def version(self) -> int:
        return self.data.task.version


class UnblockData(msgspec.Struct, kw_only=True, frozen=True):
    promise: PromiseRecord


class UnblockMsg(
    msgspec.Struct, tag="unblock", tag_field="kind", kw_only=True, frozen=True
):
    data: UnblockData

    @property
    def promise(self) -> PromiseRecord:
        """Return the settled promise -- shorthand for ``data.promise``."""
        return self.data.promise


# A parsed incoming message from the network, discriminated by its ``kind``
# field.
Message = ExecuteMsg | UnblockMsg


# =============================================================================
# Envelope helpers
# =============================================================================


def _nested_str(value: Any, *keys: str) -> str:
    """Walk nested mapping ``keys`` and return the final string, or ``""``.

    Any missing key, non-mapping node, or non-string leaf collapses to ``""``.
    """
    for key in keys:
        if not isinstance(value, dict):
            return ""
        value = value.get(key)
    return value if isinstance(value, str) else ""


# =============================================================================
# Transport
# =============================================================================


class Transport:
    """Adds JSON serialization, deserialization, and correlation validation.

    Resonate and its sub-components use the transport -- never the raw network.
    """

    def __init__(self, network: Network) -> None:
        self._network = network

    async def send(self, kind: str, corr_id: str, body: str) -> Any:
        """Send an already-serialized request, returning the parsed response."""
        logger.debug("transport send_req: %s", body)

        resp_str = await self._network.send(body)
        logger.debug("transport send_res: %s", resp_str)

        try:
            response = msgspec.json.decode(resp_str)
        except msgspec.DecodeError as exc:
            msg = f"invalid response JSON: {exc}, resp: {resp_str}"
            raise DecodingError(msg) from exc

        resp_kind = _nested_str(response, "kind")
        if resp_kind != kind:
            msg = f"response kind mismatch: expected '{kind}', got '{resp_kind}'"
            raise ServerError(500, msg)

        resp_corr = _nested_str(response, "head", "corrId")
        if resp_corr != corr_id:
            msg = f"response corrId mismatch: expected '{corr_id}', got '{resp_corr}'"
            raise ServerError(500, msg)

        return response

    def recv(self, callback: Callable[[Message], None]) -> None:
        """Register a callback for incoming messages."""

        def on_raw(raw: str) -> None:
            try:
                msg = msgspec.json.decode(raw, type=Message)
            except msgspec.MsgspecError as exc:
                logger.warning(
                    "failed to parse incoming message: %s; raw: %s", exc, raw
                )
                return
            logger.debug("transport recv: %s", raw)
            callback(msg)

        self._network.recv(on_raw)

    def network(self) -> Network:
        """Access the underlying network."""
        return self._network
