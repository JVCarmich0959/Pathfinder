#!/usr/bin/env python3
"""Simple validation of predicted event rates."""

import pandas as pd
from pathfinder import admin_event_rates, get_engine


def main():
    engine = get_engine()
    rates = admin_event_rates(engine)
    actual = pd.read_sql(
        """
        SELECT admin2_name AS admin2, events
        FROM acled_monthly_enriched
        WHERE month_start >= (date_trunc('month', CURRENT_DATE) - INTERVAL '1 month')
        """,
        engine,
    )
    df = actual.merge(rates[["admin2", "pred_rate"]], on="admin2", how="left")
    df["error"] = df["events"] - df["pred_rate"]
    rmse = (df["error"] ** 2).mean() ** 0.5
    print(f"RMSE last month = {rmse:.2f}")


if __name__ == "__main__":
    main()
