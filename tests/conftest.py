import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# absolute path to your project's main folder


if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    # adds in the list of folder when py looks for moducles