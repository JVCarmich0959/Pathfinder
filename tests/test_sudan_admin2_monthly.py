import pandas as pd

from pathfinder.etl.sudan_admin2_monthly import (
    aggregate_country_monthly,
    transform_admin2_monthly,
)


def sample_raw_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Country": ["Sudan", "Sudan"],
            "Admin1": ["Blue Nile", "Blue Nile"],
            "Admin2": ["Geisan", "Geisan"],
            "ISO3": ["sdn", "SDN"],
            "Admin2 Pcode": [" SD08109 ", "SD08109"],
            "Admin1 Pcode": ["SD08", "SD08"],
            "Month": ["January", "February"],
            "Year": [2024, 2024],
            "Events": [3, 4],
            "Fatalities": [1, 2],
        }
    )


def test_transform_admin2_monthly_parses_months():
    raw = sample_raw_df()
    tidy = transform_admin2_monthly(raw)

    assert tidy.loc[0, "month"] == 1
    assert tidy.loc[1, "month"] == 2
    assert tidy.loc[0, "iso3"] == "SDN"
    assert tidy.loc[0, "admin2_pcode"] == "SD08109"
    assert list(tidy.columns) == [
        "country",
        "iso3",
        "admin1",
        "admin1_pcode",
        "admin2",
        "admin2_pcode",
        "month",
        "year",
        "events",
        "fatalities",
    ]


def test_aggregate_country_monthly_sums_events():
    tidy = transform_admin2_monthly(sample_raw_df())
    aggregated = aggregate_country_monthly(tidy)

    assert list(aggregated.columns) == ["iso", "year", "month", "events", "fatalities"]
    january = aggregated.loc[(aggregated["year"] == 2024) & (aggregated["month"] == 1)]
    assert int(january["events"].iloc[0]) == 3
    assert int(january["fatalities"].iloc[0]) == 1
