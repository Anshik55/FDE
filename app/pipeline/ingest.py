"""File ingestion stage: loads CSV and Excel files into DataFrames and records raw source rows."""

import json
from pathlib import Path
import sqlite3
from typing import Dict, List
import pandas as pd

from app.events import Actor, EventType, emit


def ingest_files(
    conn: sqlite3.Connection,
    run_id: str,
    file_paths: List[Path | str],
) -> Dict[str, pd.DataFrame]:
    """Ingests multiple source files, stores raw source rows in DB, and emits events."""
    datasets: Dict[str, pd.DataFrame] = {}

    for path_in in file_paths:
        p = Path(path_in)
        if not p.exists():
            raise FileNotFoundError(f"Source file not found: {p}")

        filename = p.name
        if p.suffix.lower() == ".csv":
            df = pd.read_csv(p, dtype=str)  # Read as string to prevent premature coercion
        elif p.suffix.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(p, dtype=str)
        else:
            raise ValueError(f"Unsupported file type: {p.suffix}")

        # Replace NaN with empty string
        df = df.fillna("")

        # Save to source_rows
        cursor = conn.cursor()
        rows_to_insert = []
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            rows_to_insert.append((run_id, filename, int(idx) + 1, json.dumps(row_dict)))

        cursor.executemany(
            "INSERT INTO source_rows (run_id, file, row_num, raw_json) VALUES (?, ?, ?, ?)",
            rows_to_insert,
        )
        conn.commit()

        datasets[filename] = df

        emit(
            conn=conn,
            run_id=run_id,
            stage="ingest",
            event_type=EventType.FILE_INGESTED,
            actor=Actor.AGENT,
            entity_type="file",
            entity_id=filename,
            after={"rows": len(df), "columns": list(df.columns)},
            reason=f"Ingested {len(df)} rows from {filename}",
            meta={"path": str(p), "row_count": len(df), "columns": list(df.columns)},
        )

    return datasets
