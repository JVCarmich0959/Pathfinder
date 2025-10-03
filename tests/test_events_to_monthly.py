import pandas as pd

from pathfinder.etl.events_to_monthly import aggregate_events_dataframe


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
