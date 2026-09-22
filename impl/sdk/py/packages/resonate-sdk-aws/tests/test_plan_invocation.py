"""The Lambda handler's decision table, as a pure function.

Every branch here used to live inside ``lambda_handler``, interleaved with
``asyncio.run`` and an ``HttpConnection``: reaching one meant building a whole
Lambda event *and* letting the handler fall through toward real IO. So the
error branches were tested thinly and the success branch could only be reached
by mocking the drive.

:func:`~resonate_aws.plan_invocation` pulls that decision out whole. It parses
at the edge, resolves both URLs, and returns the outcome as a value --
:class:`~resonate_aws.Invocation` or :class:`~resonate_aws.Rejected` -- so each
branch is one call and one match, and the transport mapping stays in one place.
"""

from __future__ import annotations

import base64
import json
from typing import Any, cast

import pytest
from resonate_aws import Invocation, Rejected, plan_invocation


def event(
    *,
    method: str = "POST",
    body: str | None = None,
    host: str | None = "abc.lambda-url.us-east-1.on.aws",
    proto: str | None = "https",
    path: str = "/",
    domain_name: str | None = None,
    is_base64: bool = False,
) -> Any:
    """Build an HTTP payload v2 event. Fails directly if misused, never returns None."""
    headers: dict[str, str] = {}
    if host is not None:
        headers["host"] = host
    if proto is not None:
        headers["x-forwarded-proto"] = proto
    request_context: dict[str, Any] = {"http": {"method": method, "path": path}}
    if domain_name is not None:
        request_context["domainName"] = domain_name
    return {
        "requestContext": request_context,
        "headers": headers,
        "body": body,
        "isBase64Encoded": is_base64,
    }


def execute_body(
    task_id: str = "t1", version: int = 1, server_url: str | None = None
) -> str:
    message: dict[str, Any] = {
        "kind": "execute",
        "data": {"task": {"id": task_id, "version": version}},
    }
    if server_url is not None:
        message["head"] = {"serverUrl": server_url}
    return json.dumps(message)


# ── The happy path ─────────────────────────────────────────────────


def test_a_valid_request_plans_an_invocation() -> None:
    plan = plan_invocation(event(body=execute_body()), "http://server:8001", None)

    assert plan == Invocation(
        server_url="http://server:8001",
        function_url="https://abc.lambda-url.us-east-1.on.aws/",
        task_id="t1",
        task_version=1,
    )


def test_a_base64_body_is_decoded() -> None:
    encoded = base64.b64encode(execute_body("t9", 4).encode()).decode()
    plan = plan_invocation(
        event(body=encoded, is_base64=True), "http://server:8001", None
    )

    assert isinstance(plan, Invocation)
    assert (plan.task_id, plan.task_version) == ("t9", 4)


def test_a_missing_task_version_defaults_to_zero() -> None:
    body = json.dumps({"kind": "execute", "data": {"task": {"id": "t1"}}})
    plan = plan_invocation(event(body=body), "http://server:8001", None)

    assert isinstance(plan, Invocation)
    assert plan.task_version == 0


# ── Rejections, one per variant ────────────────────────────────────


def test_a_malformed_event_is_rejected_with_400() -> None:
    malformed = cast("Any", {"requestContext": "not an object"})
    plan = plan_invocation(malformed, "http://s", None)
    assert plan == Rejected(status=400, error="Malformed Lambda event.")


def test_a_non_post_method_is_rejected_with_405() -> None:
    plan = plan_invocation(event(method="GET", body=execute_body()), "http://s", None)
    assert plan == Rejected(status=405, error="Method not allowed. Use POST.")


def test_a_missing_method_is_rejected_with_405() -> None:
    evt = event(body=execute_body())
    del evt["requestContext"]["http"]["method"]
    plan = plan_invocation(evt, "http://s", None)
    assert isinstance(plan, Rejected)
    assert plan.status == 405


def test_a_missing_body_is_rejected_with_400() -> None:
    plan = plan_invocation(event(body=None), "http://s", None)
    assert plan == Rejected(status=400, error="Request body missing.")


def test_an_unparseable_body_is_rejected_with_400() -> None:
    plan = plan_invocation(event(body="{not json"), "http://s", None)
    assert isinstance(plan, Rejected)
    assert plan.status == 400
    assert "valid execute message" in plan.error


def test_a_non_execute_message_is_rejected_with_400() -> None:
    """The ``execute`` tag is the whole point: an ``unblock`` must not run here."""
    body = json.dumps({"kind": "unblock", "data": {"promise": {}}})
    plan = plan_invocation(event(body=body), "http://s", None)
    assert isinstance(plan, Rejected)
    assert plan.status == 400


def test_no_resolvable_server_url_is_rejected_with_500() -> None:
    plan = plan_invocation(event(body=execute_body()), None, None)
    assert isinstance(plan, Rejected)
    assert plan.status == 500
    assert "RESONATE_URL" in plan.error


def test_no_resolvable_function_url_is_rejected_with_500() -> None:
    plan = plan_invocation(
        event(body=execute_body(), host=None), "http://server:8001", None
    )
    assert isinstance(plan, Rejected)
    assert plan.status == 500
    assert "RESONATE_FUNCTION_URL" in plan.error


# ── URL resolution precedence ──────────────────────────────────────


def test_the_configured_server_url_beats_the_message_head() -> None:
    """The server advertises its own view of its address; it is often unroutable.

    The deployment knows the address reachable from inside the container, the
    server does not -- so an explicit value must win.
    """
    plan = plan_invocation(
        event(body=execute_body(server_url="http://localhost:8001")),
        "http://routable:8001",
        None,
    )
    assert isinstance(plan, Invocation)
    assert plan.server_url == "http://routable:8001"


def test_the_message_head_supplies_the_server_url_when_unconfigured() -> None:
    plan = plan_invocation(
        event(body=execute_body(server_url="http://from-head:8001")), None, None
    )
    assert isinstance(plan, Invocation)
    assert plan.server_url == "http://from-head:8001"


def test_the_configured_function_url_beats_the_headers() -> None:
    plan = plan_invocation(
        event(body=execute_body()), "http://s", "https://configured.example/fn"
    )
    assert isinstance(plan, Invocation)
    assert plan.function_url == "https://configured.example/fn"


def test_the_function_url_is_derived_from_forwarded_headers() -> None:
    plan = plan_invocation(
        event(body=execute_body(), host="api.example", proto="https", path="/stage/fn"),
        "http://s",
        None,
    )
    assert isinstance(plan, Invocation)
    assert plan.function_url == "https://api.example/stage/fn"


def test_a_missing_forwarded_proto_defaults_to_http() -> None:
    """``sam local`` and other unproxied contexts omit it; real gateways do not."""
    plan = plan_invocation(
        event(body=execute_body(), host="127.0.0.1:3000", proto=None), "http://s", None
    )
    assert isinstance(plan, Invocation)
    assert plan.function_url == "http://127.0.0.1:3000/"


def test_domain_name_is_the_fallback_when_no_host_header_is_sent() -> None:
    plan = plan_invocation(
        event(body=execute_body(), host=None, domain_name="api.example"),
        "http://s",
        None,
    )
    assert isinstance(plan, Invocation)
    assert plan.function_url == "https://api.example/"


def test_the_host_header_beats_domain_name() -> None:
    plan = plan_invocation(
        event(body=execute_body(), host="header.example", domain_name="ctx.example"),
        "http://s",
        None,
    )
    assert isinstance(plan, Invocation)
    assert plan.function_url == "https://header.example/"


# ── Planning is pure ───────────────────────────────────────────────


def test_planning_performs_no_io_and_is_repeatable() -> None:
    """Same inputs, same value -- no clock, no network, no environment read."""
    evt = event(body=execute_body())
    first = plan_invocation(evt, "http://s", None)
    second = plan_invocation(evt, "http://s", None)
    assert first == second


@pytest.mark.parametrize(
    "configured_server", [None, "http://a", "https://b.example:9000"]
)
def test_every_server_url_configuration_yields_a_plan_not_an_exception(
    configured_server: str | None,
) -> None:
    """The failure space is the return type: planning never raises."""
    plan = plan_invocation(event(body=execute_body()), configured_server, None)
    assert isinstance(plan, (Invocation, Rejected))
