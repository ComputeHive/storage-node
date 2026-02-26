import json
import pytest
from pathlib import Path
import httpx


from backend_client import (
    CoordinationClient,
    ContractQueryResult,
    NetworkError,
)


@pytest.fixture
def token_cache(tmp_path: Path) -> Path:
    return tmp_path / "auth.txt"
@pytest.fixture
def base_url() -> str:
    return "http://test-backend:5000"


@pytest.fixture
def sample_jwt() -> str:
    return "eyJhbGciOiJIUzI1NiJ9.test.signature"


def _mock_transport(handler):
    return httpx.MockTransport(handler)


def _make_client(base_url, transport, token_cache):

    client = CoordinationClient(
        server_url=base_url,
        request_timeout=5.0,
        token_cache_path=token_cache,
    )
    client._http = httpx.Client(transport=transport)
    return client

class TestLogin:

    def test_successful_login_returns_true(
        self, base_url, token_cache, sample_jwt
    ):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/storage/signin"
            body = json.loads(request.content)
            assert body["username"] == "alice"
            assert body["password"] == "correct"

            return httpx.Response(200, json={"token": sample_jwt})

        client = _make_client(base_url, _mock_transport(handler), token_cache)

        assert client.login("alice", "correct") is True
        assert client.token == sample_jwt



    def test_login_persists_token_to_disk(
        self, base_url, token_cache, sample_jwt
    ):
        def handler(_req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"token": sample_jwt})


        client = _make_client(base_url, _mock_transport(handler), token_cache)
        client.login("bob", "pass")

        assert token_cache.exists()

        assert token_cache.read_text(encoding="utf-8") == sample_jwt

    def test_wrong_credentials_returns_false(self, base_url, token_cache):
        def handler(_req: httpx.Request) -> httpx.Response:

            return httpx.Response(401, json={"error": "invalid"})

        client = _make_client(base_url, _mock_transport(handler), token_cache)


        assert client.login("alice", "wrong") is False
        assert client.token is None

    def test_unreachable_server_raises_network_error(
        self, base_url, token_cache
    ):

        def handler(_req: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused")


        client = _make_client(base_url, _mock_transport(handler), token_cache)
        with pytest.raises(NetworkError, match="Could not reach"):
            client.login("alice", "pass")


    def test_login_sends_json_content_type(
        self, base_url, token_cache, sample_jwt

    ):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:

            captured.update(dict(request.headers))
            return httpx.Response(200, json={"token": sample_jwt})

        client = _make_client(base_url, _mock_transport(handler), token_cache)

        client.login("bob", "secret")

        assert "application/json" in captured.get("content-type", "")

class TestSendHeartBeat:

    def test_hits_heartbeat_endpoint(
        self, base_url, token_cache, sample_jwt
    ):
        captured_path = None

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal captured_path


            captured_path = request.url.path
            return httpx.Response(200)

        client = _make_client(base_url, _mock_transport(handler), token_cache)

        client.token = sample_jwt
        client.send_heart_beat()

        assert captured_path == "/storage/heartbeat"

    def test_sends_token_in_header(
        self, base_url, token_cache, sample_jwt
    ):
        captured_token = None


        def handler(request: httpx.Request) -> httpx.Response:

            nonlocal captured_token


            captured_token = request.headers.get("token")
            return httpx.Response(200)

        client = _make_client(base_url, _mock_transport(handler), token_cache)
        client.token = sample_jwt

        client.send_heart_beat()

        assert captured_token == sample_jwt

    def test_tolerates_network_failure(
        self, base_url, token_cache, sample_jwt
    ):

        def handler(_req: httpx.Request) -> httpx.Response:

            raise httpx.ConnectError("timeout")

        client = _make_client(base_url, _mock_transport(handler), token_cache)

        client.token = sample_jwt
        client.send_heart_beat()


class TestWithdrawRequest:

    def test_sends_shard_id_in_body(
        self, base_url, token_cache, sample_jwt
    ):
        captured_body = {}

        def handler(request: httpx.Request) -> httpx.Response:

            nonlocal captured_body

            captured_body = json.loads(request.content)

            return httpx.Response(200)

        client = _make_client(base_url, _mock_transport(handler), token_cache)

        client.token = sample_jwt
        client.withdraw_request("shard-abc-123")

        assert captured_body == {"shard_id": "shard-abc-123"}

    def test_hits_withdraw_endpoint(
        self, base_url, token_cache, sample_jwt
    ):
        captured_path = None

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal captured_path

            captured_path = request.url.path
            return httpx.Response(200)


        client = _make_client(base_url, _mock_transport(handler), token_cache)

        client.token = sample_jwt
        client.withdraw_request("shard-1")


        assert captured_path == "/storage/withdraw"

    def test_tolerates_server_error(

        self, base_url, token_cache, sample_jwt
    ):
        def handler(_req: httpx.Request) -> httpx.Response:

            return httpx.Response(500, text="internal error")

        client = _make_client(base_url, _mock_transport(handler), token_cache)
        client.token = sample_jwt


        client.withdraw_request("shard-err")

class TestUpdateConnection:



    def test_sends_ip_and_port(
        self, base_url, token_cache, sample_jwt
    ):
        captured_body = {}

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal captured_body #modify the outer caputered_body
            
            captured_body = json.loads(request.content)
            return httpx.Response(200)

        client = _make_client(base_url, _mock_transport(handler), token_cache)

        client.token = sample_jwt

        client.update_connection("203.0.113.42", "50000")


        assert captured_body == {
            "ip_address": "203.0.113.42",
            "port": "50000",
        }

    def test_hits_update_connection_endpoint(
        self, base_url, token_cache, sample_jwt
    ):
        captured_path = None

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal captured_path

            captured_path = request.url.path

            return httpx.Response(200)

        client = _make_client(base_url, _mock_transport(handler), token_cache)

        client.token = sample_jwt
        client.update_connection("10.0.0.1", "8080")

        assert captured_path == "/storage/updateConnection"


    def test_tolerates_network_failure(

        self, base_url, token_cache, sample_jwt
    ):
        def handler(_req: httpx.Request) -> httpx.Response:

            raise httpx.ConnectError("down")

        client = _make_client(base_url, _mock_transport(handler), token_cache)

        client.token = sample_jwt
        client.update_connection("10.0.0.1", "8080")


class TestDoneUploading:

    def test_sends_shard_id(
        self, base_url, token_cache, sample_jwt
    ):
        captured_body = {}

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal captured_body

            captured_body = json.loads(request.content)

            return httpx.Response(200)

        client = _make_client(base_url, _mock_transport(handler), token_cache)

        client.token = sample_jwt
        client.done_uploading("shard-xyz-789")

        assert captured_body == {"shard_id": "shard-xyz-789"}

    def test_hits_shard_done_endpoint(
        self, base_url, token_cache, sample_jwt
    ):
        captured_path = None

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal captured_path
            captured_path = request.url.path
            return httpx.Response(200)

        client = _make_client(base_url, _mock_transport(handler), token_cache)

        client.token = sample_jwt
        client.done_uploading("shard-1")


        assert captured_path == "/storage/shardDoneUploading"

    def test_tolerates_network_failure(
        self, base_url, token_cache, sample_jwt
    ):

        def handler(_req: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused")

        client = _make_client(base_url, _mock_transport(handler), token_cache)
        client.token = sample_jwt

        client.done_uploading("shard-fail")

class TestGetActiveContracts:

    def test_returns_shard_list_on_success(
        self, base_url, token_cache, sample_jwt
    ):

        expected = ["shard-a", "shard-b", "shard-c"]

        def handler(_req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"shards": expected})


        client = _make_client(base_url, _mock_transport(handler), token_cache)

        client.token = sample_jwt
        result = client.get_active_contracts()

        assert isinstance(result, ContractQueryResult)

        assert result.success is True
        assert result.shard_ids == expected

    def test_empty_list_is_still_valid(

        self, base_url, token_cache, sample_jwt
    ):
        def handler(_req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"shards": []})

        client = _make_client(base_url, _mock_transport(handler), token_cache)
        client.token = sample_jwt
        result = client.get_active_contracts()

        assert result.success is True
        assert result.shard_ids == []

    def test_server_error_returns_failure(
        self, base_url, token_cache, sample_jwt
    ):
        def handler(_req: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="db timeout")

        client = _make_client(base_url, _mock_transport(handler), token_cache)
        client.token = sample_jwt
        result = client.get_active_contracts()

        assert result.success is False

        assert result.shard_ids == []

    def test_network_failure_returns_failure(
        self, base_url, token_cache, sample_jwt
    ):
        def handler(_req: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("network down")

        client = _make_client(base_url, _mock_transport(handler), token_cache)
        client.token = sample_jwt
        result = client.get_active_contracts()

        assert result.success is False
        assert result.shard_ids == []

    def test_missing_shards_key_defaults_to_empty(
        self, base_url, token_cache, sample_jwt
    ):
        def handler(_req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"other": "data"})

        client = _make_client(base_url, _mock_transport(handler), token_cache)
        client.token = sample_jwt
        result = client.get_active_contracts()

        assert result.success is True
        assert result.shard_ids == []

class TestAuthHeaders:

    def test_empty_when_no_token(self, base_url, token_cache):

        transport = _mock_transport(lambda _: httpx.Response(200))

        client = _make_client(base_url, transport, token_cache)
        assert client._auth_headers() == {}

    def test_populated_after_login(
        self, base_url, token_cache, sample_jwt
    ):
        transport = _mock_transport(lambda _: httpx.Response(200))

        client = _make_client(base_url, transport, token_cache)
        client.token = sample_jwt
        assert client._auth_headers() == {"token": sample_jwt}


class TestLifecycle:

    def test_context_manager(self, base_url, token_cache, sample_jwt):
        transport = _mock_transport(lambda _: httpx.Response(200))
        with _make_client(base_url, transport, token_cache) as client:
            client.token = sample_jwt
            client.send_heart_beat()

    def test_double_close(self, base_url, token_cache):
        transport = _mock_transport(lambda _: httpx.Response(200))
        client = _make_client(base_url, transport, token_cache)
        client.close()
        client.close()

class TestContractQueryResult:

    def test_construction(self):
        r = ContractQueryResult(shard_ids=["a", "b"], success=True)

        assert r.shard_ids == ["a", "b"]
        assert r.success is True

    def test_empty_invalid(self):

        r = ContractQueryResult(shard_ids=[], success=False)
        assert len(r.shard_ids) == 0

        assert r.success is False

    def test_equality(self):
        assert ContractQueryResult(["x"], True) == ContractQueryResult(["x"], True)

    def test_inequality(self):
        assert ContractQueryResult(["x"], True) != ContractQueryResult(["y"], True)