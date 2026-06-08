import hashlib
import json
import logging
import os
from typing import Optional

from connection_ledger import ConnectionLedger
from chunk_transfer import FragmentSender, FragmentReceiver

logger = logging.getLogger(__name__)


class IntegrityVerifier:

    def __init__(self, storage_path: str):
        self._storage_path = storage_path

    def compute_salted_hash(self, fragment_id: str, salt: str) -> str:
        file_path = os.path.join(self._storage_path, fragment_id)
        hasher = hashlib.md5()

        with open(file_path, "rb") as fh:
            content = fh.read()
            hasher.update(content)

        hasher.update(salt.encode())
        return hasher.hexdigest()


class NetworkChangeHandler:

    def __init__(self, config, backend_client, ledger: ConnectionLedger,
                 router_manager=None):
        self._config = config
        self._backend = backend_client
        self._ledger = ledger
        self._router = router_manager

    def on_public_ip_changed(self) -> None:
        self._backend.update_connection(
            self._config.public_ip, self._config.decentorage_port
        )

    def on_local_ip_changed(self, new_ip: str) -> None:
        if not self._config.local and self._router:
            logger.info("Local IP changed, updating port mappings")

            self._router.forward_port(
                self._config.decentorage_port,
                self._config.decentorage_port,
                router=None, lanip=new_ip, disable=False,
                protocol="TCP", duration=0, description=None, verbose=True,
            )

            active = self._ledger.list_all()
            for conn in active:
                self._router.forward_port(
                    conn["port"], conn["port"],
                    router=None, lanip=new_ip, disable=False,
                    protocol="TCP", duration=0, description=None, verbose=True,
                )

        self._config.local_ip = new_ip


class RequestDispatcher:

    def __init__(self, config, port_scanner, ledger: ConnectionLedger,
                 sender: FragmentSender, receiver: FragmentReceiver,
                 verifier: IntegrityVerifier):
        self._config = config
        self._scanner = port_scanner
        self._ledger = ledger
        self._sender = sender
        self._receiver = receiver
        self._verifier = verifier

    def _allocate_transfer_port(self, request: dict, connection) -> int:
        port = self._scanner.find_available_port(is_primary=False)
        logger.info("Allocated port %d for transfer", port)
        request["port"] = port

        registered = self._ledger.register(request)
        if not registered:
            logger.error("Failed to register connection in ledger")

        if connection is not None:
            connection.send(bytes(str(port), "UTF-8"))

        return port

    def dispatch(self, request: dict, connection=None) -> None:
        if request["type"] == "audit":
            digest = self._verifier.compute_salted_hash(
                request["shard_id"], request["salt"]
            )
            if connection is not None:
                connection.send(bytes(digest, "UTF-8"))
            return

        is_new = False
        if request["port"] == 0:
            is_new = True
            self._allocate_transfer_port(request, connection)

        if request["type"] == "upload":
            self._receiver.receive_data(request)
        elif request["type"] == "download":
            self._sender.send_data(request, is_new)