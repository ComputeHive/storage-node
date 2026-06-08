import logging
from pathlib import Path
from dataclasses import dataclass # auto generates __init__, __repr__for  data-holding classes
from typing import Optional
import httpx
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    #time,level,file_name,mesesage you wrote
)


class AuthenticationError(Exception):
    """Raised when login credentials are rejected"""
class NetworkError(Exception):
    """Raised when the coordination server is unreachable"""

@dataclass
class ContractQueryResult:
    shard_ids: list[str]
    success: bool
class CoordinationClient:
    _SIGNIN_ROUTE = "/storage/signin"

    _HEARTBEAT_ROUTE = "/storage/heartbeat"
    _WITHDRAW_ROUTE = "/storage/withdraw"
    _UPDATE_CONN_ROUTE = "/storage/updateConnection"

    _SHARD_DONE_ROUTE = "/storage/shardDoneUploading"
    _ACTIVE_CONTRACTS_ROUTE = "/storage/activeContracts"
    def __init__(
        self,
        server_url: str,
        request_timeout: float = 15.0,
        token_cache_path: str | Path = "Cache/auth.txt",
    ) -> None:
        self._server_url: str = server_url.rstrip("/")
        self._timeout: float = request_timeout


        self._token_path: Path = Path(token_cache_path)
        self._token: Optional[str] = None
        self._http: httpx.Client = httpx.Client(timeout=self._timeout)

    @property
    def token(self) -> Optional[str]:
        return self._token

    @token.setter
    def token(self, value: str) -> None:
        self._token = value
    def _auth_headers(self) -> dict[str, str]:
        if self._token is None:
            return {}

        return {"token": self._token}

    def _build_url(self, route: str) -> str:
        return f"{self._server_url}{route}"

    def _persist_token(self, jwt: str) -> None:
        try:

            self._token_path.parent.mkdir(parents=True, exist_ok=True)
            self._token_path.write_text(jwt, encoding="utf-8")
        except OSError as err:

            logger.error("Could not persist token: %s", err)

    def _safe_post(self, route: str, body: dict) -> Optional[httpx.Response]:
        url = self._build_url(route)
        try:
            return self._http.post(
                url, json=body, headers=self._auth_headers()

            )
        except httpx.RequestError as exc:

            logger.error("POST %s failed: %s", url, exc)
            return None

    def _safe_get(self, route: str) -> Optional[httpx.Response]:

        url = self._build_url(route)
        try:

            return self._http.get(url, headers=self._auth_headers())
        except httpx.RequestError as exc:
            logger.error("GET %s failed: %s", url, exc)
            return None


    def login(self, username: str, password: str) -> bool:

        url = self._build_url(self._SIGNIN_ROUTE)
        credentials = {"username": username, "password": password}
        try:
            response = self._http.post(url, json=credentials)

        except httpx.RequestError as exc:
            raise NetworkError(
                f"Could not reach authentication endpoint: {exc}"

            ) from exc #don't lose traceback
        if response.status_code == 200:
            self._token = response.json()["token"]

            self._persist_token(self._token)

            logger.info("Login successful for user '%s'", username)
            return True

        logger.warning("Incorrect username or password")
        return False

    def send_heart_beat(self) -> None:
        result = self._safe_get(self._HEARTBEAT_ROUTE)

        if result is None:
            logger.warning("Can not go online")

    def withdraw_request(self, shard: str) -> None:
        body = {"shard_id": shard}

        result = self._safe_post(self._WITHDRAW_ROUTE, body)
        if result is None:
            logger.warning("Can not go online")

    def update_connection(self, ip_address: str, port: str) -> None:

        body = {"ip_address": ip_address, "port": port}

        result = self._safe_post(self._UPDATE_CONN_ROUTE, body)
        if result is None:
            logger.warning("Can not go online")

    def done_uploading(self, shard_id: str) -> None:
        body = {"shard_id": shard_id}

        result = self._safe_post(self._SHARD_DONE_ROUTE, body)

        if result is None:
            logger.warning("Can not go online")

    def get_active_contracts(self) -> ContractQueryResult:
        result = self._safe_get(self._ACTIVE_CONTRACTS_ROUTE)

        if result is not None and result.status_code == 200:
            shard_list = result.json().get("shards", [])
            return ContractQueryResult(shard_ids=shard_list, success=True)

        return ContractQueryResult(shard_ids=[], success=False)

    def close(self) -> None:
        self._http.close()

    #called when write with coordiationClient -> return instaance
    def __enter__(self) -> "CoordinationClient":
        return self
    #when exit from with
    def __exit__(self, *_exc) -> None:
        self.close()

# docs
"""
# `backend_client.py` — Documentation

## Purpose

This file manages all HTTP communication between a storage node and the central coordination server, handling authentication, heartbeats, shard management, and connection updates through an OOP client with centralized error handling.

---

## Methods

| Method | Description |
|--------|-------------|
| `__init__(server_url, request_timeout, token_cache_path)` | Initializes the HTTP client with the backend URL, timeout, and token cache location. |
| `login(username, password)` | Authenticates with the backend, stores the JWT in memory and on disk, and returns `True` on success. |
| `send_heart_beat()` | Sends a GET request to signal the backend that this node is still alive. |
| `withdraw_request(shard)` | Notifies the backend that this node wants to stop storing a specific shard. |
| `update_connection(ip_address, port)` | Reports the node's current IP and port so other peers can reach it. |
| `done_uploading(shard_id)` | Confirms to the backend that a shard has been fully received and saved to disk. |
| `get_active_contracts()` | Fetches the list of shards this node is currently responsible for storing, returned as a `ContractQueryResult`. |
| `close()` | Shuts down the underlying HTTP client and releases all network resources. |

"""