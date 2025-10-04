import pandas as pd
from .db import get_engine

def road_counts_by_type(engine=None):
    if engine is None:
        engine = get_engine()
    sql = "SELECT highway, COUNT(*) AS cnt FROM sudan_roads_osm GROUP BY highway ORDER BY cnt DESC"
    return pd.read_sql(sql, engine)

def monthly_events(iso, engine=None):
    """Return monthly event counts and fatalities for one country ISO code."""
    if engine is None:
        engine = get_engine()
    sql = (
        "SELECT year, month, events, fatalities "
        "FROM sudan_monthly_violence "
        "WHERE iso = %(iso)s "
        "ORDER BY year, month"
    )
    return pd.read_sql(sql, engine, params={"iso": iso})


def monthly_totals(engine=None):
    """Aggregate events and fatalities for each month across all countries."""
    if engine is None:
        engine = get_engine()
    sql = (
        "SELECT month_start, SUM(events) AS events, SUM(fatalities) AS fatalities "
        "FROM acled_monthly_raw "
        "GROUP BY month_start "
        "ORDER BY month_start"
    )
    return pd.read_sql(sql, engine)
