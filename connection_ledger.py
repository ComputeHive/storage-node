import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class ConnectionLedger:

    def __init__(self, config, ledger_path: str = "Cache/connections.txt"):
        self._config = config
        self._filepath = Path(ledger_path)

    def _read_entries(self) -> list[dict]:
        try:
            with open(self._filepath) as fh:
                data = json.load(fh)
            return data.get("connections", [])
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            logger.error("Ledger read failed: %s", exc)
            return []

    def _write_entries(self, entries: list[dict]) -> None:
        with open(self._filepath, "w") as fh:
            json.dump({"connections": entries}, fh)

    def register(self, entry: dict) -> bool:
        self._config.semaphore.acquire()
        try:
            entries = self._read_entries()
            entries.append(entry)
            self._write_entries(entries)
            return True
        except Exception as exc:
            logger.error("Ledger register failed: %s", exc)
            return False
        finally:
            self._config.semaphore.release()

    def unregister(self, entry: dict) -> bool:
        self._config.semaphore.acquire()
        try:
            entries = self._read_entries()
            entries.remove(entry)
            self._write_entries(entries)
            return True
        except (ValueError, Exception) as exc:
            logger.error("Ledger unregister failed: %s", exc)
            return False
        finally:
            self._config.semaphore.release()

    def list_all(self) -> list[dict]:
        self._config.semaphore.acquire()
        try:
            return self._read_entries()
        finally:
            self._config.semaphore.release()