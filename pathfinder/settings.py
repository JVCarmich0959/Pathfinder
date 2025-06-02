from functools import lru_cache
from pathlib import Path
import os
import sqlalchemy as sa


@lru_cache
def engine() -> sa.Engine:
    """Return a SQLAlchemy engine from ``$DATABASE_URL`` or docker defaults."""
    url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@db:5432/pathfinder",
    )
    return sa.create_engine(url, pool_pre_ping=True)


DATA_RAW = Path(__file__).resolve().parents[1] / "data" / "raw"
