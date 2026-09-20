"""Column profiling module: analyzes structure, data types, null rates, and sample values."""

from dataclasses import asdict, dataclass
import re
import sqlite3
from typing import Any, Dict, List
import pandas as pd

from app.events import Actor, EventType, emit


@dataclass
class ColumnProfile:
    file: str
    column: str
    total_count: int
    non_null_count: int
    null_rate: float
    distinct_count: int
    uniqueness_ratio: float
    sample_values: List[str]
    inferred_type: str


def infer_simple_type(values: List[str]) -> str:
    """Heuristic type inference for raw string values."""
    clean = [v.strip() for v in values if v and v.strip()]
    if not clean:
        return "string"

    # Email
    if all("@" in v and "." in v for v in clean[:10]):
        return "email"

    # Numeric
    numeric_count = 0
    for v in clean:
        v_clean = v.replace(",", "").replace("$", "").strip()
        try:
            float(v_clean)
            numeric_count += 1
        except ValueError:
            pass
    if numeric_count / len(clean) >= 0.8:
        return "decimal"

    # Date-like (contains slashes or dashes with numbers)
    date_pattern = re.compile(r"^\d{1,4}[-/\.]\w{1,3}[-/\.]\d{2,4}$|^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}$")
    if sum(1 for v in clean if date_pattern.match(v)) / len(clean) >= 0.7:
        return "date"

    return "string"


def profile_columns(
    conn: sqlite3.Connection,
    run_id: str,
    datasets: Dict[str, pd.DataFrame],
) -> Dict[str, Dict[str, ColumnProfile]]:
    """Profile columns across all ingested datasets."""
    profiles: Dict[str, Dict[str, ColumnProfile]] = {}

    for filename, df in datasets.items():
        profiles[filename] = {}
        total_rows = len(df)

        for col in df.columns:
            series = df[col].astype(str)
            non_null = [v.strip() for v in series if v.strip() and v.strip().lower() not in ["nan", "none", "null"]]
            non_null_count = len(non_null)
            null_rate = round(1.0 - (non_null_count / total_rows), 3) if total_rows > 0 else 0.0

            unique_vals = list(dict.fromkeys(non_null))  # preserves order
            distinct_count = len(unique_vals)
            uniqueness_ratio = round(distinct_count / non_null_count, 3) if non_null_count > 0 else 0.0
            sample_values = unique_vals[:5]

            inferred = infer_simple_type(sample_values)

            prof = ColumnProfile(
                file=filename,
                column=col,
                total_count=total_rows,
                non_null_count=non_null_count,
                null_rate=null_rate,
                distinct_count=distinct_count,
                uniqueness_ratio=uniqueness_ratio,
                sample_values=sample_values,
                inferred_type=inferred,
            )
            profiles[filename][col] = prof

            emit(
                conn=conn,
                run_id=run_id,
                stage="profile",
                event_type=EventType.COLUMN_PROFILED,
                actor=Actor.AGENT,
                entity_type="column",
                entity_id=f"{filename}:{col}",
                field=col,
                after=asdict(prof),
                reason=f"Profiled {col}: {distinct_count} distinct values, inferred type '{inferred}'",
            )

    return profiles
