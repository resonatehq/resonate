"""The delivery addresses a :class:`~resonate_base.connections.Source` advertises.

A source tells the server *where to push* execute/unblock messages by handing
it two strings: a **unicast** address that reaches this process alone, and an
**anycast** address that reaches any one member of its group. The server stores
whichever it is given (in a listener registration, or in a promise's
``resonate:target`` tag) and later routes a message back to it.

The server parses an address with Go's ``url.Parse``, so an address is a URL::

    {scheme}://{host}{path}

The ``{scheme}`` selects the delivery mechanism. Everything to the right of it
is the connector's own business -- the server does not interpret it, it hands
it back to the connector that minted it.

The helpers here mint the form :class:`~resonate.connections.SSEConnection` and
:class:`~resonate.connections.LocalConnection` use::

    poll://uni@workers/7f3a    poll://any@workers/7f3a
    local://uni@default/pid    local://any@default/pid

They are a convenience, not a requirement. A connector whose destination is
already an address in its own namespace is better off saying so directly:
:class:`resonate_nats.NatsConnection` advertises
``nats://resonate.recv.workers.7f3a`` and lets the server's ``url.Parse`` hand
the subject straight back, because nesting a second addressing scheme inside a
NATS subject would buy nothing.

Note the asymmetry between the two addresses and :func:`resolve_target`: a
unicast and an anycast address both name a *concrete process*, while a resolved
target names a *group* whose members are not known to the resolver -- so it
carries no pid.

Case matters. The group and pid land in the URL **host**, which Go lowercases
during parsing, so an address minted with an uppercase group does not round
trip -- and nothing raises: the server accepts it, stores it, and the message
is simply never delivered. These helpers refuse rather than silently folding,
so the mistake surfaces where it is made.
"""

from __future__ import annotations

from resonate_base.error import InvalidIdError

__all__ = ["ANYCAST", "UNICAST", "anycast", "resolve_target", "unicast"]

#: Userinfo marking an address that reaches exactly this process.
UNICAST = "uni"

#: Userinfo marking an address that reaches any one member of the group.
ANYCAST = "any"

#: Characters that would change how ``url.Parse`` splits an address, so they
#: cannot appear in a group or pid.
_RESERVED = "@/:?#"


def _check(part: str, what: str) -> str:
    if not part:
        msg = f"{what} must not be empty"
        raise InvalidIdError(part, msg)
    if any(c in part for c in _RESERVED):
        msg = f"{what} must not contain any of {_RESERVED!r}"
        raise InvalidIdError(part, msg)
    if part != part.lower():
        msg = f"{what} must be lowercase (the server lowercases the URL host)"
        raise InvalidIdError(part, msg)
    return part


def unicast(scheme: str, group: str, pid: str) -> str:
    """Mint the address that reaches this process alone.

    >>> unicast("poll", "workers", "7f3a")
    'poll://uni@workers/7f3a'
    """
    scheme = _check(scheme, "scheme")
    return f"{scheme}://{UNICAST}@{_check(group, 'group')}/{_check(pid, 'pid')}"


def anycast(scheme: str, group: str, pid: str) -> str:
    """Mint the address that reaches any one member of this process's group.

    The pid is present but not selective: it identifies the process that minted
    the address, while delivery is to whichever group member the server picks.

    >>> anycast("poll", "workers", "7f3a")
    'poll://any@workers/7f3a'
    """
    scheme = _check(scheme, "scheme")
    return f"{scheme}://{ANYCAST}@{_check(group, 'group')}/{_check(pid, 'pid')}"


def resolve_target(scheme: str, target: str) -> str:
    """Mint the anycast address for a bare group name.

    This is what a :meth:`~resonate_base.connections.Source.target_resolver`
    returns: the caller names a group it wants work delivered to, and the source
    renders that group in its own scheme. No pid, because the caller is naming a
    group, not a process.

    >>> resolve_target("poll", "workers")
    'poll://any@workers'
    """
    return f"{_check(scheme, 'scheme')}://{ANYCAST}@{_check(target, 'target')}"
