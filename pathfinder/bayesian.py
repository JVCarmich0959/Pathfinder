"""Simple Bayesian risk model utilities."""

from __future__ import annotations
import pandas as pd
from sqlalchemy import text

from .db import get_engine


def fetch_admin_monthly(engine=None):
    """Return admin2 monthly event counts."""
    if engine is None:
        engine = get_engine()
    sql = text(
        """
        SELECT admin2_name AS admin2, events
        FROM acled_monthly_enriched
        """
    )
    return pd.read_sql(sql, engine)


def estimate_event_rate(df: pd.DataFrame, alpha: float = 1.0, beta: float = 1.0) -> pd.DataFrame:
    """Estimate monthly event rate per admin2 using a Gamma-Poisson model."""
    grouped = (
        df.groupby("admin2")
        ["events"]
        .agg(months="count", total_events="sum")
        .reset_index()
    )
    grouped["pred_rate"] = (alpha + grouped["total_events"]) / (
        beta + grouped["months"]
    )
    return grouped


def admin_event_rates(engine=None, alpha: float = 1.0, beta: float = 1.0) -> pd.DataFrame:
    df = fetch_admin_monthly(engine)
    return estimate_event_rate(df, alpha=alpha, beta=beta)


def road_segment_risk(engine=None, alpha: float = 1.0, beta: float = 1.0):
    """Return primary road segments with predicted risk scores."""
    if engine is None:
        engine = get_engine()
    rates = admin_event_rates(engine, alpha=alpha, beta=beta)
    sql = text(
        """
        SELECT r.id AS road_id,
               ST_X(ST_LineInterpolatePoint(r.geom,0.5)) AS lon,
               ST_Y(ST_LineInterpolatePoint(r.geom,0.5)) AS lat,
               ST_Length(r.geom::geography) AS length_m,
               g.admin2_name AS admin2
        FROM sudan_roads_osm r
        LEFT JOIN geo_admin2 g ON ST_Intersects(r.geom, g.geom)
        WHERE r.highway = 'primary'
        """
    )
    df = pd.read_sql(sql, engine)
    df = df.merge(rates[["admin2", "pred_rate"]], on="admin2", how="left")
    df["risk"] = df["pred_rate"].fillna(0) / df["length_m"].replace({0: 1})
    return df


def update_risk_table(engine=None, alpha: float = 1.0, beta: float = 1.0):
    """Rebuild the road_risk_scores table."""
    df = road_segment_risk(engine=engine, alpha=alpha, beta=beta)
    if engine is None:
        engine = get_engine()
    df[["road_id", "risk"]].to_sql(
        "road_risk_scores", engine, if_exists="replace", index=False
    )
