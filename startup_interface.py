import json
import logging
import sys
import threading
from typing import Optional

logger = logging.getLogger(__name__)
class BannerRenderer:

    @staticmethod
    def display(title: str = "CERASTORAGE") -> None:
        try:
            from PIL import Image, ImageDraw, ImageFont
            import numpy as np

            font = ImageFont.load_default()

            # Measure using a temp canvas — most accurate method
            temp = Image.new("1", (1, 1))
            temp_draw = ImageDraw.Draw(temp)
            bbox = temp_draw.textbbox((0, 0), title, font=font)

            # bbox = (left, top, right, bottom)
            width = bbox[2] + 1   # +1 for safety margin
            height = bbox[3] + 1

            canvas = Image.new("1", (width, height), "black")
            artist = ImageDraw.Draw(canvas)
            artist.text((0, 0), title, "white", font=font)

            pixel_grid = np.array(canvas, dtype=np.uint8)
            symbols = np.array([" ", "#"], dtype="U1")[pixel_grid]
            rows = symbols.view("U" + str(symbols.shape[1])).flatten()
            print("\n".join(rows))
            print("\n\n")
        except ImportError:
            print(f"=== {title} ===\n")
# class BannerRenderer:
#
#     @staticmethod
#     def display(title: str = "DECENTORAGE") -> None:
#         try:
#             from PIL import Image, ImageDraw, ImageFont
#             import numpy as np
#
#             font = ImageFont.load_default()
#             dimensions = font.getsize(title)
#             canvas = Image.new("1", dimensions, "black")
#             artist = ImageDraw.Draw(canvas)
#             artist.text((0, 0), title, "white", font=font)
#
#             pixel_grid = np.array(canvas, dtype=np.uint8)
#             symbols = np.array([" ", "#"], dtype="U1")[pixel_grid]
#             rows = symbols.view("U" + str(symbols.shape[1])).flatten()
#             print("\n".join(rows))
#             print("\n\n")
#         except ImportError:
#             print(f"=== {title} ===\n")


class AuthenticationPrompt:

    def __init__(self, backend_client):
        self._backend = backend_client

    def run(self, username: Optional[str] = None,
            password: Optional[str] = None) -> None:
        if username and password:
            success = self._backend.login(username, password)
            if not success:
                logger.error("Invalid credentials provided")
                sys.exit(-1)
            return

        authenticated = False
        while not authenticated:
            entered_user = input("Username: ")
            entered_pass = input("Password: ")
            authenticated = self._backend.login(entered_user, entered_pass)


class SessionRecovery:

    def __init__(self, dispatcher, ledger_path: str = "Cache/connections.txt"):
        self._dispatcher = dispatcher
        self._ledger_path = ledger_path

    def resume_pending(self) -> int:
        resumed_count = 0
        try:
            with open(self._ledger_path) as fh:
                data = json.load(fh)

            pending = data.get("connections", [])
            for entry in pending:
                task = dict(entry)
                worker = threading.Thread(
                    target=self._dispatcher.dispatch,
                    args=(task,),
                )
                worker.start()
                resumed_count += 1

            logger.info("Resumed %d pending transfers", resumed_count)
        except (FileNotFoundError, json.JSONDecodeError, Exception) as exc:
            logger.error("Session recovery failed: %s", exc)

        return resumed_count