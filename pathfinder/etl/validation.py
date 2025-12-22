"""Data validation and provenance helpers for ETL jobs.

These utilities are intentionally lightweight so they can run in
constrained environments (no heavy dependencies). Validation reports are
simple dataclasses that can be written to disk for auditing.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import pandas as pd


SEVERITY_ERROR = "error"
SEVERITY_WARN = "warn"


@dataclass
class ValidationIssue:
    field: str
    message: str
    severity: str = SEVERITY_ERROR


@dataclass
class ValidationReport:
    source: str
    row_count: int
    started_at: dt.datetime
    finished_at: dt.datetime
    issues: List[ValidationIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(issue.severity != SEVERITY_ERROR for issue in self.issues)

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "row_count": self.row_count,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "issues": [issue.__dict__ for issue in self.issues],
            "status": "ok" if self.ok else "failed",
        }


def _as_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def check_required_columns(df: pd.DataFrame, required: Sequence[str]) -> List[ValidationIssue]:
    missing = [col for col in required if col not in df.columns]
    if missing:
        return [ValidationIssue(field="columns", message=f"Missing columns: {missing}")]
    return []


def check_missing_values(df: pd.DataFrame, columns: Sequence[str]) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []
    for col in columns:
        missing = df[col].isna().sum()
        if missing > 0:
            issues.append(
                ValidationIssue(
                    field=col,
                    message=f"{missing} missing values in {col}",
                    severity=SEVERITY_WARN,
                )
            )
    return issues


def check_date_window(
    df: pd.DataFrame,
    column: str,
    start: Optional[dt.date],
    end: Optional[dt.date],
) -> List[ValidationIssue]:
    if column not in df.columns:
        return [ValidationIssue(field=column, message="Date column not found")]
    dates = _as_datetime(df[column])
    issues: List[ValidationIssue] = []
    if dates.isna().any():
        issues.append(
            ValidationIssue(
                field=column,
                message=f"{dates.isna().sum()} unparsable dates in {column}",
            )
        )
    if start:
        too_old = (dates.dt.date < start).sum()
        if too_old:
            issues.append(
                ValidationIssue(
                    field=column,
                    message=f"{too_old} rows older than {start}",
                    severity=SEVERITY_WARN,
                )
            )
    if end:
        too_new = (dates.dt.date > end).sum()
        if too_new:
            issues.append(
                ValidationIssue(
                    field=column,
                    message=f"{too_new} rows newer than {end}",
                    severity=SEVERITY_WARN,
                )
            )
    return issues


def check_duplicates(df: pd.DataFrame, subset: Sequence[str]) -> List[ValidationIssue]:
    if not subset:
        return []
    dupes = df.duplicated(subset=subset).sum()
    if dupes:
        return [
            ValidationIssue(
                field=",".join(subset),
                message=f"{dupes} duplicate rows on {subset}",
                severity=SEVERITY_WARN,
            )
        ]
    return []


def run_validation(
    df: pd.DataFrame,
    source: str,
    required_columns: Sequence[str],
    date_column: Optional[str] = None,
    start: Optional[dt.date] = None,
    end: Optional[dt.date] = None,
    duplicate_subset: Optional[Sequence[str]] = None,
    missing_columns_warn: Optional[Sequence[str]] = None,
) -> ValidationReport:
    started = dt.datetime.now(dt.timezone.utc)
    issues: List[ValidationIssue] = []
    issues.extend(check_required_columns(df, required_columns))
    if date_column:
        issues.extend(check_date_window(df, date_column, start, end))
    if missing_columns_warn:
        issues.extend(check_missing_values(df, missing_columns_warn))
    if duplicate_subset:
        issues.extend(check_duplicates(df, duplicate_subset))
    finished = dt.datetime.now(dt.timezone.utc)
    return ValidationReport(
        source=source,
        row_count=len(df),
        started_at=started,
        finished_at=finished,
        issues=issues,
    )


def write_report(report: ValidationReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([report.to_dict()])
    if path.exists():
        existing = pd.read_csv(path)
        df = pd.concat([existing, df], ignore_index=True)
    df.to_csv(path, index=False)


def quarantine_file(src: Path, quarantine_dir: Path) -> Path:
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    dest = quarantine_dir / src.name
    src.replace(dest)
    return dest
