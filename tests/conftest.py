"""
Test setup: put the repo root on sys.path (the services import `shared` and
`services` as top-level packages, like PYTHONPATH=/app in the Dockerfiles),
and give Settings the one required variable. Nothing here connects to Kafka,
Postgres or Redis; the tests use fake consumers.
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test")
