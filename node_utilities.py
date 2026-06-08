import json
import logging
import random
import os
from pathlib import Path
from typing import Optional

import httpx
import psutil
import zmq

from router_gateway import RouterPortManager

logger = logging.getLogger(__name__)


class PortScanner:

    def __init__(self, config, router_manager: Optional[RouterPortManager] = None):
        self._config = config
        self._router = router_manager

    def check_zmq_bind(self, port_number: int) -> bool:
        occupied = False
        ctx = zmq.Context()
        sock = ctx.socket(zmq.PAIR)#Bidirectional
        try:
            sock.bind(f"tcp://127.0.0.1:{port_number}")
        except zmq.ZMQError:
            occupied = True
        finally:
            sock.close()
            ctx.term()
        return occupied

    def is_port_in_use(self, port_number: int) -> bool:
        zmq_occupied = self.check_zmq_bind(port_number)

        if self._config.local or self._config.hosted:
            return zmq_occupied

        router_occupied = False
        if self._router is not None:
            # todo:
            router_occupied = self._router.is_port_open(port_number)

        return zmq_occupied or router_occupied

    def find_available_port(self, is_primary: bool = False) -> int:
        candidate = int(self._config.starting_port)

        while True:
            if is_primary:
                if not self.is_port_in_use(candidate):
                    self._config.decentorage_port = candidate
                    break
                candidate += 1
            else:
                candidate = random.randint(50100, 60000)
                if not self.is_port_in_use(candidate):
                    break

        if not self._config.local and not self._config.hosted and self._router:
            #todo:
            self._router.forward_port(
                candidate, candidate, router=None, lanip=None,
                disable=False, protocol="TCP", duration=0,
                description=None, verbose=False,
            )

        return candidate


class NetworkResolver:

    _PUBLIC_IP_ENDPOINT = "https://api.ipify.org"

    def __init__(self, request_timeout: float = 10.0):
        self._timeout = request_timeout

    def fetch_public_ip(self) -> str:
        try:
            resp = httpx.get(self._PUBLIC_IP_ENDPOINT, timeout=self._timeout)
            return resp.text.strip()
        except httpx.RequestError as exc:
            logger.error("Failed to resolve public IP: %s", exc)
            raise

    @staticmethod
    def fetch_disk_space_kb() -> float:
        usage = psutil.disk_usage("/")
        return usage.free / (2 ** 10)


class ConfigFileManager:

    def __init__(self, config, cache_dir: str = "Cache"):
        self._config = config
        self._cache_path = Path(cache_dir)
        self._config_file = self._cache_path / "config.txt"
        self._connections_file = self._cache_path / "connections.txt"
        self._auth_file = self._cache_path / "auth.txt"

    def read_config(self) -> None:
        with open(self._config_file, "r") as handle:
            lines = handle.readlines()

        self._config.local_ip = lines[0].replace(" ", "").strip()
        self._config.decentorage_port = int(lines[1])

    def write_config(self) -> None:
        with open(self._config_file, "w") as handle:
            handle.write(self._config.local_ip + "\n")
            handle.write(str(self._config.decentorage_port))

    def initialize_directories(self, data_dir: str = "Data") -> None:
        data_path = Path(data_dir)
        if not data_path.is_dir():
            data_path.mkdir(parents=True)

        if not self._cache_path.is_dir():
            self._cache_path.mkdir(parents=True)

            empty_connections = {"connections": []}
            with open(self._connections_file, "w") as handle:
                json.dump(empty_connections, handle)

            self._auth_file.touch()


class NodeBootstrapper:

    def __init__(
        self,
        config,
        port_scanner: PortScanner,
        network_resolver: NetworkResolver,
        config_manager: ConfigFileManager,
        router_manager: Optional[RouterPortManager] = None,
    ):
        self._config = config
        self._scanner = port_scanner
        self._resolver = network_resolver
        self._cfg_manager = config_manager
        self._router = router_manager

    def setup_first_run(self) -> None:
        self._cfg_manager.initialize_directories(self._config.data_directory)

        config_path = self._cfg_manager._config_file
        if not config_path.exists():
            if self._router is not None:
                local = self._router.get_my_ip()
            else:
                local = "127.0.0.1"

            self._config.local_ip = local
            self._config.decentorage_port = self._scanner.find_available_port(
                is_primary=True
            )
            self._cfg_manager.write_config()

        if not self._config.local:
            try:
                self._config.public_ip = self._resolver.fetch_public_ip()
            except Exception:
                logger.warning("Check your internet connection")

    def verify_primary_port(self) -> None:
        occupied = self._scanner.check_zmq_bind(self._config.decentorage_port)

        if occupied:
            self._scanner.find_available_port(is_primary=True)
        else:
            if (
                not self._config.local
                and not self._config.hosted
                and self._router is not None
            ):
                self._router.forward_port(
                    self._config.decentorage_port,
                    self._config.decentorage_port,
                    router=None, lanip=None, disable=False,
                    protocol="TCP", duration=0,
                    description=None, verbose=False,
                )