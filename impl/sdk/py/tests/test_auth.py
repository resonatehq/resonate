from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from resonate import Resonate
from resonate.message_sources import Poller
from resonate.stores import LocalStore, RemoteStore

# ─── RemoteStore token auth ───


class TestRemoteStoreTokenAuth:
    def test_token_param(self) -> None:
        store = RemoteStore(url="http://localhost:8001", token="my-token")  # noqa: S106
        assert store._token == "my-token"  # noqa: SLF001, S105

    def test_no_token(self) -> None:
        store = RemoteStore(url="http://localhost:8001")
        assert store._token is None  # noqa: SLF001

    def test_bearer_header_applied(self) -> None:
        store = RemoteStore(url="http://localhost:8001", token="my-token")  # noqa: S106
        req = MagicMock()
        req.headers = {}

        with patch.object(store, "_retry_policy") as mock_policy:
            mock_policy.next.return_value = None

            mock_session = MagicMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"id": "test"}
            mock_response.raise_for_status.return_value = None
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)
            mock_session.send.return_value = mock_response

            with patch("resonate.stores.remote.Session", return_value=mock_session):
                store.call(req)

        assert req.headers["Authorization"] == "Bearer my-token"

    def test_basic_auth_fallback(self) -> None:
        store = RemoteStore(url="http://localhost:8001", auth=("user", "pass"))
        req = MagicMock()
        req.headers = {}

        with patch.object(store, "_retry_policy") as mock_policy:
            mock_policy.next.return_value = None

            mock_session = MagicMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"id": "test"}
            mock_response.raise_for_status.return_value = None
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)
            mock_session.send.return_value = mock_response

            with patch("resonate.stores.remote.Session", return_value=mock_session):
                store.call(req)

        req.prepare_auth.assert_called_once_with(("user", "pass"))
        assert "Authorization" not in req.headers

    def test_token_priority_over_basic_auth(self) -> None:
        store = RemoteStore(url="http://localhost:8001", auth=("user", "pass"), token="my-token")  # noqa: S106
        req = MagicMock()
        req.headers = {}

        with patch.object(store, "_retry_policy") as mock_policy:
            mock_policy.next.return_value = None

            mock_session = MagicMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"id": "test"}
            mock_response.raise_for_status.return_value = None
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)
            mock_session.send.return_value = mock_response

            with patch("resonate.stores.remote.Session", return_value=mock_session):
                store.call(req)

        assert req.headers["Authorization"] == "Bearer my-token"
        req.prepare_auth.assert_not_called()


# ─── Poller token auth ───


class TestPollerTokenAuth:
    def test_token_param(self) -> None:
        poller = Poller(group="default", id="test", url="http://localhost:8001", token="my-token")  # noqa: S106
        assert poller._token == "my-token"  # noqa: SLF001, S105

    def test_no_token(self) -> None:
        poller = Poller(group="default", id="test", url="http://localhost:8001")
        assert poller._token is None  # noqa: SLF001

    def test_url_format(self) -> None:
        poller = Poller(group="mygroup", id="myid", url="http://localhost:8001")
        assert poller.url == "http://localhost:8001/poll/mygroup/myid"

    def test_url_default(self) -> None:
        poller = Poller(group="default", id="test")
        assert poller.url == "http://localhost:8001/poll/default/test"


# ─── RemoteStore URL ───


class TestRemoteStoreUrl:
    def test_url_param(self) -> None:
        store = RemoteStore(url="http://myserver:9000")
        assert store.url == "http://myserver:9000"

    def test_url_default(self) -> None:
        store = RemoteStore()
        assert store.url == "http://localhost:8001"


# ─── Resonate auto-detection ───


class TestResonateAutoDetection:
    def test_default_is_local(self) -> None:
        """Resonate() with no args defaults to local mode."""
        r = Resonate()
        assert isinstance(r._store, LocalStore)  # noqa: SLF001

    def test_explicit_url_goes_remote(self) -> None:
        """Resonate(url=...) creates RemoteStore + Poller."""
        r = Resonate(url="http://localhost:8001")
        assert isinstance(r._store, RemoteStore)  # noqa: SLF001
        assert isinstance(r._message_source, Poller)  # noqa: SLF001
        assert r._store.url == "http://localhost:8001"  # noqa: SLF001

    @patch.dict(os.environ, {"RESONATE_URL": "http://env-server:8001"}, clear=False)
    def test_env_url_goes_remote(self) -> None:
        """RESONATE_URL env var triggers remote mode."""
        r = Resonate()
        assert isinstance(r._store, RemoteStore)  # noqa: SLF001
        assert r._store.url == "http://env-server:8001"  # noqa: SLF001

    @patch.dict(os.environ, {"RESONATE_HOST": "my-host", "RESONATE_PORT": "9000"}, clear=False)
    def test_env_host_port_goes_remote(self) -> None:
        """RESONATE_HOST + RESONATE_PORT env vars construct URL and trigger remote mode."""
        r = Resonate()
        assert isinstance(r._store, RemoteStore)  # noqa: SLF001
        assert r._store.url == "http://my-host:9000"  # noqa: SLF001

    @patch.dict(os.environ, {"RESONATE_HOST": "my-host"}, clear=False)
    def test_env_host_default_port(self) -> None:
        """RESONATE_HOST without RESONATE_PORT defaults to port 8001."""
        r = Resonate()
        assert isinstance(r._store, RemoteStore)  # noqa: SLF001
        assert r._store.url == "http://my-host:8001"  # noqa: SLF001

    @patch.dict(os.environ, {"RESONATE_HOST": "my-host", "RESONATE_SCHEME": "https"}, clear=False)
    def test_env_scheme(self) -> None:
        """RESONATE_SCHEME env var is used in URL construction."""
        r = Resonate()
        assert isinstance(r._store, RemoteStore)  # noqa: SLF001
        assert r._store.url == "https://my-host:8001"  # noqa: SLF001

    def test_explicit_url_overrides_env(self) -> None:
        """Explicit url param takes priority over env vars."""
        with patch.dict(os.environ, {"RESONATE_URL": "http://env-server:8001"}, clear=False):
            r = Resonate(url="http://explicit:9000")
            assert isinstance(r._store, RemoteStore)  # noqa: SLF001
            assert r._store.url == "http://explicit:9000"  # noqa: SLF001


# ─── Resonate auth resolution ───


def _remote(r: Resonate) -> tuple[RemoteStore, Poller]:
    """Narrow store/message_source types for remote-mode assertions."""
    assert isinstance(r._store, RemoteStore)  # noqa: SLF001
    assert isinstance(r._message_source, Poller)  # noqa: SLF001
    return r._store, r._message_source  # noqa: SLF001


class TestResonateAuthResolution:
    def test_explicit_token(self) -> None:
        """Explicit token is passed to store and poller."""
        r = Resonate(url="http://localhost:8001", token="my-token")  # noqa: S106
        store, ms = _remote(r)
        assert store._token == "my-token"  # noqa: SLF001, S105
        assert ms._token == "my-token"  # noqa: SLF001, S105

    @patch.dict(os.environ, {"RESONATE_TOKEN": "env-token"}, clear=False)
    def test_env_token(self) -> None:
        """RESONATE_TOKEN env var is used as fallback."""
        r = Resonate(url="http://localhost:8001")
        store, ms = _remote(r)
        assert store._token == "env-token"  # noqa: SLF001, S105
        assert ms._token == "env-token"  # noqa: SLF001, S105

    def test_explicit_token_overrides_env(self) -> None:
        """Explicit token takes priority over env var."""
        with patch.dict(os.environ, {"RESONATE_TOKEN": "env-token"}, clear=False):
            r = Resonate(url="http://localhost:8001", token="explicit-token")  # noqa: S106
            store, _ = _remote(r)
            assert store._token == "explicit-token"  # noqa: SLF001, S105

    def test_explicit_auth(self) -> None:
        """Explicit auth is passed to store and poller."""
        r = Resonate(url="http://localhost:8001", auth=("user", "pass"))
        store, ms = _remote(r)
        assert store._auth == ("user", "pass")  # noqa: SLF001
        assert ms._auth == ("user", "pass")  # noqa: SLF001

    @patch.dict(os.environ, {"RESONATE_USERNAME": "env-user", "RESONATE_PASSWORD": "env-pass"}, clear=False)
    def test_env_auth(self) -> None:
        """RESONATE_USERNAME/PASSWORD env vars are used as fallback."""
        r = Resonate(url="http://localhost:8001")
        store, ms = _remote(r)
        assert store._auth == ("env-user", "env-pass")  # noqa: SLF001
        assert ms._auth == ("env-user", "env-pass")  # noqa: SLF001

    @patch.dict(os.environ, {"RESONATE_USERNAME": "env-user"}, clear=False)
    def test_env_username_default_password(self) -> None:
        """RESONATE_USERNAME without PASSWORD defaults to empty password."""
        r = Resonate(url="http://localhost:8001")
        store, _ = _remote(r)
        assert store._auth == ("env-user", "")  # noqa: SLF001

    def test_no_auth(self) -> None:
        """No auth configured results in None for both token and auth."""
        r = Resonate(url="http://localhost:8001")
        store, _ = _remote(r)
        assert store._token is None  # noqa: SLF001
        assert store._auth is None  # noqa: SLF001

    def test_token_and_auth_both_passed(self) -> None:
        """Both token and auth can be passed; token takes priority in requests."""
        r = Resonate(url="http://localhost:8001", token="my-token", auth=("user", "pass"))  # noqa: S106
        store, _ = _remote(r)
        assert store._token == "my-token"  # noqa: SLF001, S105
        assert store._auth == ("user", "pass")  # noqa: SLF001


# ─── Resonate.local/remote removed ───


class TestResonateApiSurface:
    def test_local_removed(self) -> None:
        """Resonate.local() class method has been removed."""
        assert not hasattr(Resonate, "local")

    def test_remote_removed(self) -> None:
        """Resonate.remote() class method has been removed."""
        assert not hasattr(Resonate, "remote")
