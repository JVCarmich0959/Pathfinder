import pandas as pd
from .db import get_engine

def road_counts_by_type(engine=None):
    if engine is None:
        engine = get_engine()
    sql = "SELECT highway, COUNT(*) AS cnt FROM sudan_roads_osm GROUP BY highway ORDER BY cnt DESC"
    return pd.read_sql(sql, engine)

def monthly_events(iso, engine=None):
    if engine is None:
        engine = get_engine()
    sql = f"""
        SELECT year, month, events, fatalities
        FROM sa_monthly_violence
        WHERE iso = {iso}
        ORDER BY year, month
    """
    return pd.read_sql(sql, engine)
