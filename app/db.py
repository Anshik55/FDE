"""Database schema and connection management with append-only event log triggers."""

import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List, Optional

DB_PATH = Path(__file__).resolve().parent.parent / "migration.db"


def get_db_connection(db_path: Optional[Path | str] = None) -> sqlite3.Connection:
    path = db_path or DB_PATH
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")

    # Ensure schema exists
    table_check = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='runs'"
    ).fetchone()
    if not table_check:
        _create_schema(conn)

    return conn


def _create_schema(conn: sqlite3.Connection):
    with conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
            id TEXT PRIMARY KEY,
            client_id TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            config_json TEXT
        );

        CREATE TABLE IF NOT EXISTS source_rows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL REFERENCES runs(id),
            file TEXT NOT NULL,
            row_num INTEGER NOT NULL,
            raw_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL REFERENCES runs(id),
            ts TEXT NOT NULL,
            stage TEXT NOT NULL,
            type TEXT NOT NULL,
            actor TEXT NOT NULL,
            entity_type TEXT,
            entity_id TEXT,
            field TEXT,
            before TEXT,
            after TEXT,
            reason TEXT,
            score REAL,
            escalation_id TEXT,
            meta_json TEXT
        );

        -- Enforce append-only integrity on events table
        CREATE TRIGGER IF NOT EXISTS prevent_events_update
        BEFORE UPDATE ON events
        BEGIN
            SELECT RAISE(ABORT, 'events table is strictly append-only: updates are prohibited');
        END;

        CREATE TRIGGER IF NOT EXISTS prevent_events_delete
        BEFORE DELETE ON events
        BEGIN
            SELECT RAISE(ABORT, 'events table is strictly append-only: deletes are prohibited');
        END;

        CREATE TABLE IF NOT EXISTS escalations (
            id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL REFERENCES runs(id),
            group_key TEXT NOT NULL,
            type TEXT NOT NULL,
            status TEXT NOT NULL,  -- pending | resolved | rejected
            context_json TEXT NOT NULL,
            proposal_json TEXT,
            affected_ids_json TEXT,
            created_at TEXT NOT NULL,
            resolved_at TEXT,
            resolved_by TEXT,
            resolution_json TEXT
        );

        CREATE TABLE IF NOT EXISTS rules (
            id TEXT PRIMARY KEY,
            client_id TEXT NOT NULL,
            kind TEXT NOT NULL,  -- mapping | enum_value | date_format | sensitive_mapping_confirmed
            fingerprint TEXT NOT NULL,
            action_json TEXT NOT NULL,
            source_escalation_id TEXT,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            times_applied INTEGER NOT NULL DEFAULT 0,
            UNIQUE(client_id, kind, fingerprint)
        );

        CREATE TABLE IF NOT EXISTS canonical_records (
            id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL REFERENCES runs(id),
            canonical_key TEXT NOT NULL,
            data_json TEXT NOT NULL,
            provenance_json TEXT NOT NULL,
            status TEXT NOT NULL  -- pending | ready | pushed | failed | rejected
        );

        CREATE TABLE IF NOT EXISTS push_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL REFERENCES runs(id),
            record_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            attempt INTEGER NOT NULL,
            http_status INTEGER,
            target_id TEXT,
            error TEXT,
            ts TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS llm_cache (
            prompt_hash TEXT PRIMARY KEY,
            request_json TEXT NOT NULL,
            response_json TEXT NOT NULL,
            model TEXT NOT NULL,
            ts TEXT NOT NULL
        );

        -- Stub target API store (acts as external Darwinbox/HR system)
        CREATE TABLE IF NOT EXISTS target_records (
            id TEXT PRIMARY KEY,
            employee_id TEXT UNIQUE NOT NULL,
            payload_json TEXT NOT NULL,
            idempotency_key TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS stub_failure_config (
            key TEXT PRIMARY KEY,
            config_json TEXT NOT NULL
        );
        """)
    return conn


def init_db(db_path: Optional[Path | str] = None) -> sqlite3.Connection:
    """Initialize database and ensure tables exist."""
    return get_db_connection(db_path)


def reset_database(db_path: Optional[Path | str] = None) -> sqlite3.Connection:
    """Reset database cleanly by recreating tables."""
    path = Path(db_path or DB_PATH)
    for ext in ["", "-wal", "-shm"]:
        p = Path(f"{path}{ext}")
        if p.exists():
            try:
                p.unlink()
            except Exception:
                pass
    return init_db(path)


