import threading
import logging
import typer

from node_config import NodeConfig
from backend_client import CoordinationClient
from router_gateway import RouterPortManager
from node_utilities import (
    PortScanner, NetworkResolver,
    ConfigFileManager, NodeBootstrapper,
)
from connection_ledger import ConnectionLedger
from chunk_transfer import FragmentSender, FragmentReceiver
from request_dispatcher import (
    IntegrityVerifier, NetworkChangeHandler, RequestDispatcher,
)
from service_workers import (
    CommandListener, PulseEmitter, NetworkWatcher, ContractEnforcer,
)
from startup_interface import (
    BannerRenderer, AuthenticationPrompt, SessionRecovery,
)

logger = logging.getLogger(__name__)
app = typer.Typer()


class StorageNode:

    def __init__(self, config: NodeConfig):
        self._config = config

        self._router = RouterPortManager()
        self._backend = CoordinationClient(server_url=config.server_url)
        self._scanner = PortScanner(config, self._router)
        self._resolver = NetworkResolver()
        self._cfg_manager = ConfigFileManager(config)
        self._bootstrapper = NodeBootstrapper(
            config, self._scanner, self._resolver,
            self._cfg_manager, self._router,
        )
        self._ledger = ConnectionLedger(config)

        self._sender = FragmentSender(config, self._ledger, self._router)
        self._receiver = FragmentReceiver(
            config, self._ledger, self._backend, self._router,
        )
        self._verifier = IntegrityVerifier(config.data_directory)
        self._change_handler = NetworkChangeHandler(
            config, self._backend, self._ledger, self._router,
        )
        self._dispatcher = RequestDispatcher(
            config, self._scanner, self._ledger,
            self._sender, self._receiver, self._verifier,
        )

    def initialize(self, username: str, password: str) -> None:
        BannerRenderer.display()
        self._bootstrapper.setup_first_run()

        auth = AuthenticationPrompt(self._backend)
        auth.run(username, password)

        self._cfg_manager.read_config()

        watcher = NetworkWatcher(
            self._config, self._resolver,
            self._change_handler, self._cfg_manager, self._router,
        )
        watcher.check_once()

        self._bootstrapper.verify_primary_port()
        self._backend.update_connection(
            self._config.public_ip, str(self._config.decentorage_port)
        )

        recovery = SessionRecovery(self._dispatcher)
        recovery.resume_pending()

    def run(self) -> None:
        listener = CommandListener(self._config, self._dispatcher)
        pulse = PulseEmitter(self._backend)
        enforcer = ContractEnforcer(self._config, self._backend)

        threads = [
            threading.Thread(target=listener.run_forever, daemon=True),
            threading.Thread(target=pulse.run_forever, daemon=True),
            threading.Thread(target=enforcer.run_forever, daemon=True),
        ]

        for t in threads:
            t.start()

        for t in threads:
            t.join()


@app.command()
def main(
    username: str = typer.Option(None),
    password: str = typer.Option(None),
    starting_port: int = typer.Option(50000),
):
    config = NodeConfig()
    config.starting_port = starting_port

    node = StorageNode(config)
    node.initialize(username, password)
    node.run()


if __name__ == "__main__":
    app()