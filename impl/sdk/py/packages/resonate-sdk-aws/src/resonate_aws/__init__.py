"""AWS Lambda entrypoint for running Resonate functions serverless.

:class:`Resonate` here is the serverless twin of
:class:`resonate.resonate.Resonate`. It shares that class's construction-time
wiring (codec, registry, dependency map, retry policy) and its
:meth:`register` / :meth:`with_dependency` surface, but drops everything a
serverless worker cannot do:

* No persistent event loop, no SSE listener, no background tasks. Work is not
  *polled*; the Resonate server *pushes* exactly one ``execute`` message per
  Lambda invocation over HTTP, and the invocation returns once the task is
  ``done`` or ``suspended``.
* No async heartbeat. A Lambda cannot beat a lease in the background, so a
  :class:`~resonate.heartbeat.NoopHeartbeat` is used and the lease ``ttl``
  must be set safely above the function's configured timeout -- the server
  re-delivers the task if the invocation dies without settling it.
* No ``run`` / ``rpc`` / ``get`` client methods. Those *create* top-level work;
  a worker only *executes* work handed to it. A Lambda drives one task through
  :class:`~resonate.core.Core` and reports the outcome.

The per-invocation wiring (the :class:`~resonate.connections.HttpConnection` back
to the server, the :class:`~resonate.core.Core`) is built fresh inside the
returned handler because both the server URL and this function's own public
URL are only known once the request arrives. Everything reusable across
invocations -- codec, registry, dependencies -- is built once at construction
and closed over.

Usage::

    from resonate_aws import Resonate

    resonate = Resonate()

    @resonate.register
    def greet(ctx, name: str) -> str:
        return f"hello {name}"

    lambda_handler = resonate.handler()
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
import os
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Concatenate, overload

import msgspec

from resonate.codec import Codec, NoopEncryptor
from resonate.connections import HttpConnection
from resonate.core import Core
from resonate.dependencies import DependencyMap
from resonate.error import ApplicationError
from resonate.heartbeat import NoopHeartbeat
from resonate.observability import logging_observer
from resonate.registry import Registry
from resonate.retry import Exponential
from resonate.send import Sender
from resonate.timing import now_ms, sleep
from resonate.transport import ExecuteData, Transport

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from aws_lambda_typing.context import Context as LambdaContext
    from aws_lambda_typing.events import APIGatewayProxyEventV2

    from resonate.codec import Encryptor
    from resonate.context import Context, TargetResolver
    from resonate.observability import Observer
    from resonate.retry import RetryPolicy
    from resonate.timing import Clock, Sleeper

logger = logging.getLogger(__name__)

#: Default per-task lease duration. Five minutes, matching the TypeScript FaaS
#: shim. A serverless worker cannot heartbeat, so the server holds the lease
#: for the whole window: set this at least as high as the function's configured
#: timeout, or the server re-delivers a task the Lambda is still running.
DEFAULT_TTL = timedelta(minutes=5)

#: The JSON response body returned to the server for each terminal outcome of
#: :meth:`~resonate.core.Core.on_message`.
_COMPLETED_BODY = {"status": "completed"}
_SUSPENDED_BODY = {"status": "suspended"}


# ── Typed views of the inbound request ─────────────────────────────────────────
#
# The Lambda runtime hands the handler an already-parsed ``dict`` (the Function
# URL / API Gateway HTTP payload v2), and the request *body* is a JSON string
# carrying the Resonate execute message. Rather than walking either by hand we
# decode both into these structs -- ``msgspec.convert`` for the event dict,
# ``msgspec.json.decode`` for the body string. Each models only the fields the
# shim consumes; msgspec ignores the rest.


class _Http(msgspec.Struct, kw_only=True, frozen=True):
    method: str | None = None
    path: str = "/"


class _RequestContext(msgspec.Struct, kw_only=True, frozen=True, rename="camel"):
    http: _Http = msgspec.field(default_factory=_Http)
    #: Fallback source for this function's own host when no ``host`` header is
    #: present (some emulators omit it). API Gateway v2 always sets this.
    domain_name: str | None = None


class _Headers(
    msgspec.Struct,
    kw_only=True,
    frozen=True,
    rename={"x_forwarded_proto": "x-forwarded-proto"},
):
    host: str | None = None
    x_forwarded_proto: str | None = None


class _LambdaEvent(msgspec.Struct, kw_only=True, frozen=True, rename="camel"):
    """The parts of an HTTP payload v2 event the shim reads."""

    request_context: _RequestContext = msgspec.field(default_factory=_RequestContext)
    headers: _Headers = msgspec.field(default_factory=_Headers)
    body: str | None = None
    is_base64_encoded: bool = False


class _MessageHead(msgspec.Struct, kw_only=True, frozen=True, rename="camel"):
    #: Optional server URL carried on the pushed message; when absent the
    #: constructor / ``RESONATE_URL`` value is used instead.
    server_url: str | None = None


class _ExecuteEnvelope(
    msgspec.Struct, tag="execute", tag_field="kind", kw_only=True, frozen=True
):
    """The execute message body. The ``execute`` tag rejects other kinds."""

    data: ExecuteData
    head: _MessageHead = msgspec.field(default_factory=_MessageHead)


# ── The request-to-wiring decision, as a value ─────────────────────────────
#
# Everything the handler decides *before* touching the network -- is this a
# POST, does the body parse, which server URL, which function URL -- is pure.
# :func:`plan_invocation` performs that whole decision and returns it as a
# value: :class:`Invocation` when the request is good, :class:`Rejected` when
# it is not, carrying the status and message the caller should reply with.
#
# That is what makes each branch a one-line test. Previously they were
# reachable only by constructing a full Lambda event *and* letting the handler
# fall through to ``asyncio.run``; now the whole decision table is exercised by
# calling one function and matching on the result.


class Invocation(msgspec.Struct, frozen=True, kw_only=True):
    """A validated request, resolved down to what :meth:`Resonate._drive` needs."""

    server_url: str
    function_url: str
    task_id: str
    task_version: int


class Rejected(msgspec.Struct, frozen=True, kw_only=True):
    """A request that must not run, and the response it earns."""

    status: int
    error: str


#: The outcome of planning one invocation. A closed union, so the transport
#: mapping below can match it exhaustively.
type Plan = Invocation | Rejected


def plan_invocation(
    event: APIGatewayProxyEventV2,
    configured_server_url: str | None,
    configured_function_url: str | None,
) -> Plan:
    """Decide what (if anything) this request should run. Pure -- no IO.

    Parses at the edge (the raw event dict and the raw body string both become
    typed values here) and resolves both URLs from the configured values plus
    the request's own headers. Every failure is a returned :class:`Rejected`,
    never an exception, so the failure space is the return type.
    """
    try:
        req = msgspec.convert(event, type=_LambdaEvent)
    except msgspec.ValidationError:
        return Rejected(status=400, error="Malformed Lambda event.")

    if req.request_context.http.method != "POST":
        return Rejected(status=405, error="Method not allowed. Use POST.")

    raw = _decode_body(req)
    if raw is None:
        return Rejected(status=400, error="Request body missing.")

    try:
        msg = msgspec.json.decode(raw, type=_ExecuteEnvelope)
    except msgspec.MsgspecError:
        return Rejected(
            status=400, error="Request body must be a valid execute message."
        )

    # Server URL: prefer the constructor / ``RESONATE_URL`` value, falling back
    # to a ``serverUrl`` carried on the message head. An explicitly configured
    # address must win: the server embeds its *own* view of its URL in the head
    # (e.g. ``localhost:8001``), which is unreachable from inside a Lambda
    # container -- the deployment knows the routable address, the server does
    # not. HttpConnection needs this to send acquire/fulfill/suspend back.
    server_url = configured_server_url or msg.head.server_url
    if not server_url:
        return Rejected(
            status=500,
            error=(
                "Cannot determine Resonate server URL: set RESONATE_URL or pass url=."
            ),
        )

    # This function's own public URL, used as the anycast address so child
    # tasks are routed back to this same function. An explicit ``function_url``
    # / ``RESONATE_FUNCTION_URL`` wins; otherwise it is reconstructed from the
    # forwarded headers the platform sets (works behind both API Gateway and
    # Lambda Function URLs). Only ``host`` is required: a missing
    # ``x-forwarded-proto`` means an unproxied context (e.g. ``sam local``), so
    # it defaults to ``http`` -- real API Gateway / Function URLs always send it.
    function_url = configured_function_url
    if not function_url:
        host = req.headers.host or req.request_context.domain_name
        if not host:
            return Rejected(
                status=500,
                error=(
                    "Cannot determine function URL: no host header or domainName. "
                    "Set RESONATE_FUNCTION_URL or pass function_url=."
                ),
            )
        proto = req.headers.x_forwarded_proto or "http"
        function_url = f"{proto}://{host}{req.request_context.http.path}"

    return Invocation(
        server_url=server_url,
        function_url=function_url,
        task_id=msg.data.task.id,
        task_version=msg.data.task.version,
    )


class Resonate:
    """Serverless entry point for the Resonate SDK on AWS Lambda.

    Construct one at module import, register functions and dependencies on it,
    then export :meth:`handler` as the Lambda entrypoint. Unlike
    :class:`resonate.resonate.Resonate` this holds no network, no event loop,
    and spawns no background work; it is a passive container of registrations
    that mints a per-invocation :class:`~resonate.core.Core` when the handler
    is called.
    """

    def __init__(
        self,
        *,
        url: str | None = None,
        function_url: str | None = None,
        pid: str | None = None,
        ttl: timedelta | None = None,
        token: str | None = None,
        encryptor: Encryptor | None = None,
        retry_policy: RetryPolicy | None = None,
        env: Mapping[str, str] | None = None,
        clock: Clock = now_ms,
        sleeper: Sleeper = sleep,
        observer: Observer = logging_observer,
    ) -> None:
        """Build the reusable, invocation-independent wiring.

        ``url`` is the Resonate server URL used to talk *back* to the server;
        it falls back to the ``RESONATE_URL`` env var, then to a ``serverUrl``
        carried on the pushed message's ``head`` (see :meth:`handler`).

        ``function_url`` is this function's own public URL, used as the anycast
        address child tasks are routed back to. When unset it is reconstructed
        per-invocation from the platform's forwarded headers
        (``x-forwarded-proto`` + ``host``); set it explicitly -- or via the
        ``RESONATE_FUNCTION_URL`` env var -- when those headers are unavailable,
        e.g. under ``sam local``.

        ``ttl`` defaults to :data:`DEFAULT_TTL`. ``token`` falls back to
        ``RESONATE_TOKEN``. ``retry_policy`` is the SDK-wide default for a
        pure-leaf root failure, mirroring
        :class:`resonate.resonate.Resonate`.
        """
        environ: Mapping[str, str] = env if env is not None else os.environ
        self._url = url if url is not None else environ.get("RESONATE_URL")
        self._function_url = (
            function_url
            if function_url is not None
            else environ.get("RESONATE_FUNCTION_URL")
        )
        self._pid = pid if pid is not None else uuid.uuid4().hex
        self._ttl = ttl if ttl is not None else DEFAULT_TTL
        self._auth = token if token is not None else environ.get("RESONATE_TOKEN")
        self._clock = clock
        self._sleeper = sleeper
        self._observer = observer

        self._codec = Codec(encryptor if encryptor is not None else NoopEncryptor())
        self._registry = Registry()
        self._deps = DependencyMap()
        self._retry_policy = (
            retry_policy
            if retry_policy is not None
            else Exponential(delay=1, max_delay=(1 << 63) - 1, factor=2, max_retries=30)
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def with_dependency(self, value: Any) -> Resonate:
        """Store a typed application dependency, shared with every context.

        Dependencies are keyed by their concrete type and read back inside a
        function via ``ctx.get_dependency(SomeType)``. Add them **before**
        exporting :meth:`handler`.
        """
        self._deps.insert(value)
        return self

    @overload
    def register[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], T],
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> Callable[Concatenate[Context, P], T]: ...
    @overload
    def register[**P, T](
        self,
        fn: None = None,
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> Callable[
        [Callable[Concatenate[Context, P], T]], Callable[Concatenate[Context, P], T]
    ]: ...
    def register(
        self,
        fn: Callable[Concatenate[Context, ...], Any] | None = None,
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> Any:
        """Register a durable function. Usable as a decorator.

        Identical semantics to :meth:`resonate.resonate.Resonate.register`:
        ``name`` defaults to ``fn.__name__``, ``version`` to ``1``, and the
        same name may be registered at several versions. Returns ``fn``
        unchanged so it works bare (``@resonate.register``) or parameterized
        (``@resonate.register(name="...", version=2)``).
        """
        if fn is None:
            return lambda f: self.register(
                f, name=name, version=version, retry_policy=retry_policy
            )
        reg_name = name if name is not None else getattr(fn, "__name__", "")
        if not reg_name:
            msg = "register: a name is required for an anonymous function"
            raise ApplicationError(msg)
        self._registry.register(reg_name, fn, version, retry_policy)
        return fn

    def handler(
        self,
    ) -> Callable[[APIGatewayProxyEventV2, LambdaContext], dict[str, Any]]:
        """Return the AWS Lambda handler for a Function URL (HTTP payload v2).

        The returned coroutine-free handler processes a single ``execute``
        message per invocation: it validates the HTTP request, decodes the
        message, builds an :class:`~resonate.connections.HttpConnection` back to
        the server and a :class:`~resonate.core.Core`, drives the task to
        ``done`` or ``suspended`` under a fresh event loop, and returns a JSON
        status. Child tasks dispatched by the workflow are routed back to this
        same function (see the resolver below), so a recursive workflow
        re-invokes this Lambda.
        """

        def lambda_handler(
            event: APIGatewayProxyEventV2, context: LambdaContext
        ) -> dict[str, Any]:
            try:
                plan = plan_invocation(event, self._url, self._function_url)
                match plan:
                    case Rejected(status=status, error=error):
                        return _response(status, {"error": error})
                    case Invocation():
                        logger.info("faas: function_url=%s", plan.function_url)
                        outcome = asyncio.run(
                            self._drive(
                                plan.server_url,
                                plan.function_url,
                                plan.task_id,
                                plan.task_version,
                            )
                        )
            except Exception as error:  # report any failure back to the server
                logger.exception("faas: handler failed")
                return _response(500, {"error": f"Handler failed: {error}"})

            body = _COMPLETED_BODY if outcome == "done" else _SUSPENDED_BODY
            return _response(200, body)

        return lambda_handler

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _drive(
        self, server_url: str, function_url: str, task_id: str, version: int
    ) -> str:
        """Build the per-invocation wiring and run the task to a terminal state.

        An :class:`~resonate.connections.HttpConnection` (request/response only)
        is all that is needed because the server pushes work over HTTP --
        there is no message source to open. The resolver routes any non-URL
        child target back to ``function_url`` so recursive sub-tasks re-invoke
        this Lambda; explicit URL targets pass through unchanged.
        """
        network = HttpConnection(url=server_url, auth=self._auth, sleeper=self._sleeper)
        sender = Sender(
            Transport(network, observer=self._observer),
            self._auth,
            observer=self._observer,
        )

        core = Core(
            # One ``Sender`` satisfies both narrow ports structurally.
            sender=sender,
            fencing=sender,
            codec=self._codec,
            registry=self._registry,
            resolver=_self_routing_resolver(function_url),
            heartbeat=NoopHeartbeat(),
            pid=self._pid,
            ttl=_safe_ttl_ms(self._ttl),
            deps=self._deps,
            retry_policy=self._retry_policy,
            clock=self._clock,
            sleeper=self._sleeper,
            observer=self._observer,
        )

        await network.start()
        try:
            return await core.on_message(task_id, version)
        finally:
            with contextlib.suppress(Exception):
                await network.stop()


# ── Module-level helpers ───────────────────────────────────────────────────────


def _decode_body(event: _LambdaEvent) -> str | None:
    """Return the request body as text, base64-decoding it when flagged.

    Function URLs and API Gateway set ``isBase64Encoded`` for bodies they treat
    as binary; the JSON envelope must be decoded before parsing.
    """
    if event.body is None:
        return None
    if event.is_base64_encoded:
        return base64.b64decode(event.body).decode("utf-8")
    return event.body


def _self_routing_resolver(function_url: str) -> TargetResolver:
    """Build a resolver that routes non-URL child targets back to this function.

    An explicit URL target (``scheme://...``) passes through unchanged so a
    workflow can dispatch to another worker; every other target -- a bare group
    name, or none at all -- resolves to ``function_url`` so recursive sub-tasks
    are pushed back to this same Lambda.
    """

    def resolver(target: str | None) -> str:
        if target and "://" in target:
            return target
        return function_url

    return resolver


def _response(status_code: int, body: dict[str, Any]) -> dict[str, Any]:
    """Build a Lambda Function URL response with a JSON-string body."""
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": msgspec.json.encode(body).decode("utf-8"),
    }


def _safe_ttl_ms(ttl: timedelta) -> int:
    ms = int(ttl.total_seconds() * 1000)
    return ms if ms > 0 else (1 << 50)
