from sqlalchemy import create_engine
import os, functools

@functools.lru_cache
def get_engine():
    url = (
        f"postgresql://{os.getenv('POSTGRES_USER','postgres')}:"
        f"{os.getenv('POSTGRES_PASSWORD','postgres')}"
        f"@{os.getenv('POSTGRES_HOST','db')}:5432/"
        f"{os.getenv('POSTGRES_DB','pathfinder')}"
    )
    return create_engine(url, pool_pre_ping=True)
