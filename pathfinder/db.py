# pathfinder/db.py
from functools import lru_cache
from sqlalchemy import create_engine
import os

@lru_cache(maxsize=None)
def get_engine() -> "sqlalchemy.Engine":
    """
    Return (and cache) a SQL-Alchemy engine.

    Priority:
    1. $DATABASE_URL  – full URL wins if set.
    2. Individual POSTGRES_* env vars, with sensible defaults.
    """
    # 1 ▸ entire URL provided → just use it
    if "DATABASE_URL" in os.environ:
        url = os.environ["DATABASE_URL"]

    # 2 ▸ otherwise compose the pieces
    else:
        url = (
            f"postgresql://{os.getenv('POSTGRES_USER',     'postgres')}:"
            f"{os.getenv('POSTGRES_PASSWORD', 'postgres')}@"
            f"{os.getenv('POSTGRES_HOST',     'localhost')}:"
            f"{os.getenv('POSTGRES_PORT',     '5432')}/"
            f"{os.getenv('POSTGRES_DB',       'pathfinder')}"
        )

    return create_engine(url, pool_pre_ping=True)
