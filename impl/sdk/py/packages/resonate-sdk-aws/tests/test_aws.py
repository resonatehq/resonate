"""Behaviour tests for :mod:`resonate_aws`.

The AWS FaaS shim is a serverless worker: the Resonate server pushes one
``execute`` message per Lambda invocation over HTTP, and the handler drives
that single task to ``done`` / ``suspended`` and reports a JSON status.

These tests exercise the parts that live entirely in the handler -- request
validation, event/body decoding into the msgspec structs, function-URL
reconstruction, and the terminal status mapping -- by stubbing the one method
that needs a live server (:meth:`Resonate._drive`). The self-routing resolver
and the send-only network behaviour are tested directly.
"""

from __future__ import annotations

import asyncio
import base64
from typing import TYPE_CHECKING, Any, cast

import msgspec
import pytest
from resonate_aws import Resonate, _self_routing_resolver

from resonate.error import AlreadyRegisteredError, ApplicationError
from resonate.network import HttpNetwork

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context as LambdaContext
    from aws_lambda_typing.events import APIGatewayProxyEventV2

    from resonate.context import Context

# -- constants ----------------------------------------------------------------

_SERVER_URL = "http://resonate.example:8001"
_EXECUTE_BODY = '{"kind":"execute","data":{"task":{"id":"t1","version":2}}}'

#: The Lambda runtime passes a context object the shim never reads; a cast
#: ``None`` satisfies the handler's typed signature without fabricating one.
_CONTEXT = cast("LambdaContext", None)


def _event(
    *,
    method: str = "POST",
    path: str = "/",
    body: str | None = _EXECUTE_BODY,
    host: str | None = "fn.example",
    proto: str | None = "https",
    is_base64: bool = False,
) -> APIGatewayProxyEventV2:
    """Build an HTTP payload v2 event. Inert setup, so shared across tests."""
    headers: dict[str, str] = {}
    if host is not None:
        headers["host"] = host
    if proto is not None:
        headers["x-forwarded-proto"] = proto
    event: dict[str, Any] = {
        "requestContext": {"http": {"method": method, "path": path}},
        "headers": headers,
        "isBase64Encoded": is_base64,
    }
    if body is not None:
        event["body"] = body
    return cast("APIGatewayProxyEventV2", event)


def _body(resp: dict[str, Any]) -> Any:
    """Decode the JSON-string body of a handler response."""
    return msgspec.json.decode(resp["body"])


# -- request validation -------------------------------------------------------


def test_handler_rejects_non_post() -> None:
    resp = Resonate(url=_SERVER_URL).handler()(_event(method="GET"), _CONTEXT)
    assert resp["statusCode"] == 405


def test_handler_rejects_missing_body() -> None:
    resp = Resonate(url=_SERVER_URL).handler()(_event(body=None), _CONTEXT)
    assert resp["statusCode"] == 400
    assert "missing" in _body(resp)["error"].lower()


def test_handler_rejects_non_execute_message() -> None:
    body = '{"kind":"unblock","data":{"promise":{}}}'
    resp = Resonate(url=_SERVER_URL).handler()(_event(body=body), _CONTEXT)
    assert resp["statusCode"] == 400
    assert "execute message" in _body(resp)["error"]


def test_handler_rejects_malformed_body() -> None:
    resp = Resonate(url=_SERVER_URL).handler()(_event(body="not json"), _CONTEXT)
    assert resp["statusCode"] == 400


def test_handler_missing_server_url_returns_500(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RESONATE_URL", raising=False)
    # No url= and no head.serverUrl -> nowhere to send server requests.
    resp = Resonate().handler()(_event(), _CONTEXT)
    assert resp["statusCode"] == 500
    assert "server URL" in _body(resp)["error"]


def test_handler_missing_host_returns_500() -> None:
    # Only ``host`` is required to reconstruct the function URL; without it there
    # is no address to route child tasks back to.
    resp = Resonate(url=_SERVER_URL).handler()(_event(host=None), _CONTEXT)
    assert resp["statusCode"] == 500
    assert "function URL" in _body(resp)["error"]


def test_handler_missing_proto_defaults_to_http(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A missing ``x-forwarded-proto`` (e.g. under ``sam local``) is not fatal:
    # the proto defaults to ``http`` so no RESONATE_FUNCTION_URL is needed.
    r = Resonate(url=_SERVER_URL)
    seen: dict[str, Any] = {}

    async def fake_drive(server_url: str, function_url: str, *_: Any) -> str:
        seen["function_url"] = function_url
        return "done"

    monkeypatch.setattr(r, "_drive", fake_drive)
    resp = r.handler()(
        _event(host="127.0.0.1:3000", proto=None, path="/hello"), _CONTEXT
    )
    assert resp["statusCode"] == 200
    assert seen["function_url"] == "http://127.0.0.1:3000/hello"


# -- successful drive (server-facing call stubbed) ----------------------------


def test_handler_drives_task_and_maps_done(monkeypatch: pytest.MonkeyPatch) -> None:
    r = Resonate(url=_SERVER_URL)
    seen: dict[str, Any] = {}

    async def fake_drive(
        server_url: str, function_url: str, task_id: str, version: int
    ) -> str:
        seen.update(
            server_url=server_url,
            function_url=function_url,
            task_id=task_id,
            version=version,
        )
        return "done"

    monkeypatch.setattr(r, "_drive", fake_drive)
    resp = r.handler()(_event(path="/prod"), _CONTEXT)

    assert resp["statusCode"] == 200
    assert _body(resp) == {"status": "completed"}
    # Server URL from the constructor, function URL rebuilt from the headers +
    # path, task id/version decoded from the execute body.
    assert seen == {
        "server_url": _SERVER_URL,
        "function_url": "https://fn.example/prod",
        "task_id": "t1",
        "version": 2,
    }


def test_handler_maps_suspended(monkeypatch: pytest.MonkeyPatch) -> None:
    r = Resonate(url=_SERVER_URL)

    async def fake_drive(*_: Any) -> str:
        return "suspended"

    monkeypatch.setattr(r, "_drive", fake_drive)
    resp = r.handler()(_event(), _CONTEXT)
    assert resp["statusCode"] == 200
    assert _body(resp) == {"status": "suspended"}


def test_handler_constructor_server_url_overrides_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A configured url= must win over the ``serverUrl`` the server embeds in the
    # head: the server advertises its own (often container-local) view of its
    # address, only the deployment knows the routable one.
    r = Resonate(url=_SERVER_URL)
    seen: dict[str, Any] = {}

    async def fake_drive(server_url: str, *_: Any) -> str:
        seen["server_url"] = server_url
        return "done"

    monkeypatch.setattr(r, "_drive", fake_drive)
    body = (
        '{"kind":"execute","head":{"serverUrl":"http://from-head:9999"},'
        '"data":{"task":{"id":"t1","version":1}}}'
    )
    r.handler()(_event(body=body), _CONTEXT)
    assert seen["server_url"] == _SERVER_URL


def test_handler_head_server_url_used_when_no_constructor_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # With no url= / RESONATE_URL configured, the head ``serverUrl`` is the
    # fallback source of the server address.
    monkeypatch.delenv("RESONATE_URL", raising=False)
    r = Resonate()
    seen: dict[str, Any] = {}

    async def fake_drive(server_url: str, *_: Any) -> str:
        seen["server_url"] = server_url
        return "done"

    monkeypatch.setattr(r, "_drive", fake_drive)
    body = (
        '{"kind":"execute","head":{"serverUrl":"http://from-head:9999"},'
        '"data":{"task":{"id":"t1","version":1}}}'
    )
    r.handler()(_event(body=body), _CONTEXT)
    assert seen["server_url"] == "http://from-head:9999"


def test_handler_decodes_base64_body(monkeypatch: pytest.MonkeyPatch) -> None:
    r = Resonate(url=_SERVER_URL)
    seen: dict[str, Any] = {}

    async def fake_drive(
        server_url: str, function_url: str, task_id: str, v: int
    ) -> str:
        seen["task_id"] = task_id
        return "done"

    monkeypatch.setattr(r, "_drive", fake_drive)
    encoded = base64.b64encode(_EXECUTE_BODY.encode()).decode()
    resp = r.handler()(_event(body=encoded, is_base64=True), _CONTEXT)
    assert resp["statusCode"] == 200
    assert seen["task_id"] == "t1"


def test_handler_reports_drive_failure_as_500(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    r = Resonate(url=_SERVER_URL)

    async def boom(*_: Any) -> str:
        msg = "kaboom"
        raise RuntimeError(msg)

    monkeypatch.setattr(r, "_drive", boom)
    resp = r.handler()(_event(), _CONTEXT)
    assert resp["statusCode"] == 500
    assert "kaboom" in _body(resp)["error"]


# -- self-routing resolver ----------------------------------------------------


def test_self_routing_resolver_passes_urls_through() -> None:
    resolve = _self_routing_resolver("https://fn.example/")
    assert resolve("https://other.example/") == "https://other.example/"
    assert resolve("poll://any@group/pid") == "poll://any@group/pid"


def test_self_routing_resolver_routes_bare_targets_to_self() -> None:
    resolve = _self_routing_resolver("https://fn.example/")
    assert resolve("some-group") == "https://fn.example/"
    assert resolve(None) == "https://fn.example/"
    assert resolve("") == "https://fn.example/"


# -- registration -------------------------------------------------------------


def test_register_returns_function_unchanged() -> None:
    r = Resonate(url=_SERVER_URL)

    def greet(ctx: Context, name: str) -> str:
        return f"hi {name}"

    # register returns the function unchanged and records it under its name.
    assert r.register(greet) is greet
    assert r._registry.get("greet", 1) is not None


def test_register_duplicate_raises() -> None:
    r = Resonate(url=_SERVER_URL)

    @r.register
    def greet(ctx: Context) -> None: ...

    with pytest.raises(AlreadyRegisteredError):
        r.register(greet)


def test_register_anonymous_requires_name() -> None:
    r = Resonate(url=_SERVER_URL)

    class Callable:  # a callable object carries no ``__name__``
        def __call__(self, ctx: Context) -> None: ...

    with pytest.raises(ApplicationError):
        r.register(Callable())


def test_with_dependency_is_readable_by_type() -> None:
    r = Resonate(url=_SERVER_URL)
    r.with_dependency("a-client")
    assert r._deps.get(str) == "a-client"


# -- send-only network --------------------------------------------------------


def test_send_only_network_skips_sse_listener() -> None:
    async def run() -> None:
        send_only = HttpNetwork(url=_SERVER_URL, send_only=True)
        await send_only.start()
        # No poll connection is opened for a serverless worker.
        assert send_only._sse_handle is None
        await send_only.stop()

        polling = HttpNetwork(url=_SERVER_URL)
        await polling.start()
        assert polling._sse_handle is not None
        await polling.stop()

    asyncio.run(run())
