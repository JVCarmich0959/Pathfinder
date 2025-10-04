import pandas as pd
import pytest
import sqlalchemy as sa
from sqlalchemy import text

from pathfinder.etl.events_to_monthly import (
    aggregate_events_dataframe,
    load_events_from_csv,
    validate_identifier,
    write_monthly_table,
)


def test_aggregate_events_dataframe_basic():
    raw = pd.DataFrame(
        {
            "iso": ["sdn", "sdn", "tcd"],
            "country": ["Sudan", "Sudan", "Chad"],
            "event_date": ["2024-01-15", "2024-01-20", "2024-02-01"],
            "fatalities": [1, 0, 3],
        }
    )
    aggregated = aggregate_events_dataframe(raw)
    assert len(aggregated) == 2
    january = aggregated[(aggregated["iso"] == "SDN") & (aggregated["month"] == 1)]
    assert january["events"].iloc[0] == 2
    assert january["fatalities"].iloc[0] == 1


def test_aggregate_events_dataframe_handles_invalid_rows():
    raw = pd.DataFrame(
        {
            "iso": ["sdn", None],
            "country": ["Sudan", ""],
            "event_date": ["2024-01-15", "bad"],
            "fatalities": [None, 5],
        }
    )
    aggregated = aggregate_events_dataframe(raw)
    assert len(aggregated) == 1
    row = aggregated.iloc[0]
    assert row["iso"] == "SDN"
    assert row["fatalities"] == 0


def test_write_monthly_table_truncates_when_empty(tmp_path):
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'monthly.db'}")
    monthly = pd.DataFrame(
        {
            "iso": ["SDN"],
            "country": ["Sudan"],
            "year": [2024],
            "month": [1],
            "events": [2],
            "fatalities": [1],
        }
    )

    write_monthly_table(engine, monthly, destination_table="monthly_totals")
    empty = monthly.iloc[0:0]
    write_monthly_table(engine, empty, destination_table="monthly_totals")

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM monthly_totals"))
        assert count.scalar_one() == 0


def test_validate_identifier_rejects_injection():
    with pytest.raises(ValueError):
        validate_identifier("events_raw; DROP TABLE events_raw")


def test_load_events_from_csv(tmp_path):
    csv_path = tmp_path / "events.csv"
    pd.DataFrame(
        {
            "iso": ["sdn", "tcd"],
            "event_date": ["2024-01-01", "2024-02-01"],
            "country": ["Sudan", "Chad"],
            "fatalities": [0, 1],
        }
    ).to_csv(csv_path, index=False)

    frame = load_events_from_csv(csv_path)
    assert set(frame.columns) >= {"iso", "event_date"}
    assert len(frame) == 2


def test_load_events_from_csv_missing_required(tmp_path):
    csv_path = tmp_path / "events.csv"
    pd.DataFrame({"country": ["Sudan"], "fatalities": [0]}).to_csv(csv_path, index=False)

    with pytest.raises(ValueError):
        load_events_from_csv(csv_path)
