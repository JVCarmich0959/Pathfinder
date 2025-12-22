import datetime as dt
from pathlib import Path

import pandas as pd

from pathfinder.etl.validation import run_validation, quarantine_file


def test_validation_detects_missing_columns_and_dates(tmp_path: Path):
    df = pd.DataFrame(
        {
            "event_id": [1, 1],
            "event_date": ["2020-01-01", "bad-date"],
            "country": ["A", "A"],
        }
    )
    report = run_validation(
        df,
        source="test",
        required_columns=["event_id", "event_date", "country", "admin1"],
        date_column="event_date",
        start=dt.date(2019, 12, 31),
        end=dt.date(2020, 1, 2),
        duplicate_subset=["event_id"],
    )
    assert not report.ok
    fields = {issue.field for issue in report.issues}
    assert "columns" in fields
    assert "event_date" in fields
    assert "event_id" in next(issue for issue in report.issues if "duplicate" in issue.message).field


def test_quarantine_moves_file(tmp_path: Path):
    src = tmp_path / "test.csv"
    src.write_text("a,b\n1,2")
    quarantined = quarantine_file(src, tmp_path / "q")
    assert quarantined.exists()
    assert quarantined.read_text().startswith("a,b")
