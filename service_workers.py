import socket
import json
import logging
import os
import threading
from time import sleep
from typing import Optional

logger = logging.getLogger(__name__)


class CommandListener:

    def __init__(self, config, dispatcher):
        self._config = config
        self._dispatcher = dispatcher

    def run_forever(self) -> None:
        logger.info("Listening on port %d", self._config.decentorage_port)
        srv = socket.socket()
        srv.bind((self._config.local_ip, self._config.decentorage_port))
        srv.listen(5)

        while True:
            conn, addr = srv.accept()
            raw = conn.recv(1024).decode("utf-8")
            incoming = json.loads(raw)
            logger.info("Incoming request: %s", incoming["type"])

            worker = threading.Thread(
                target=self._dispatcher.dispatch,
                args=(incoming, conn),
            )
            worker.start()


class PulseEmitter:

    def __init__(self, backend_client, interval_seconds: int = 300):
        self._backend = backend_client
        self._interval = interval_seconds

    def run_forever(self) -> None:
        while True:
            self._backend.send_heart_beat()
            sleep(self._interval)


class NetworkWatcher:

    def __init__(self, config, network_resolver, change_handler,
                 config_manager, router_manager=None,
                 poll_interval: int = 10):
        self._config = config
        self._resolver = network_resolver
        self._handler = change_handler
        self._cfg_manager = config_manager
        self._router = router_manager
        self._interval = poll_interval

    def check_once(self) -> None:
        if not self._config.local:
            try:
                current_public = self._resolver.fetch_public_ip()
                if current_public != self._config.public_ip:
                    self._config.public_ip = current_public
                    self._handler.on_public_ip_changed()
            except Exception:
                logger.warning("Failed to check public IP")

        current_local = (
            self._router.get_my_ip() if self._router
            else self._config.local_ip
        )

        if current_local and current_local != self._config.local_ip:
            self._handler.on_local_ip_changed(current_local)
            self._cfg_manager.write_config()

        if self._config.local:
            self._config.public_ip = self._config.local_ip

    def run_forever(self) -> None:
        while True:
            self.check_once()
            sleep(self._interval)


class ContractEnforcer:

    def __init__(self, config, backend_client,
                 cycle_seconds: int = 43200):
        self._config = config
        self._backend = backend_client
        self._cycle = cycle_seconds

    def _collect_stored_fragments(self) -> list[str]:
        try:
            return os.listdir(self._config.data_directory)
        except FileNotFoundError:
            return []

    def _purge_and_claim(self) -> None:
        stored = self._collect_stored_fragments()
        result = self._backend.get_active_contracts()

        if hasattr(result, "shard_ids"):
            active_ids = result.shard_ids
            query_ok = result.success
        else:
            active_ids, query_ok = result

        for fragment in stored:
            if query_ok and fragment not in active_ids:
                target = os.path.join(self._config.data_directory, fragment)
                os.unlink(target)
                logger.info("Removed expired fragment: %s", fragment)
                continue

            self._backend.withdraw_request(fragment)

    def run_forever(self) -> None:
        while True:
            self._purge_and_claim()
            sleep(self._cycle)