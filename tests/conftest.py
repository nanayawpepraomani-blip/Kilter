"""Shared test fixtures.

Adds the project root to sys.path so tests can import top-level modules
(swift_core, recon_engine, etc.) without needing to install the package.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
