"""Event sourcing spine: emit and tail append-only pipeline events."""

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Dict, List, Optional


class EventType:
    RUN_STARTED = "RUN_STARTED"
    FILE_INGESTED = "FILE_INGESTED"
    COLUMN_PROFILED = "COLUMN_PROFILED"
    MAPPING_PROPOSED = "MAPPING_PROPOSED"
    MAPPING_ACCEPTED = "MAPPING_ACCEPTED"
    LLM_CALLED = "LLM_CALLED"
    VALUE_NORMALIZED = "VALUE_NORMALIZED"
    DATE_FORMAT_RESOLVED = "DATE_FORMAT_RESOLVED"
    RECORDS_MERGED = "RECORDS_MERGED"
    CONFLICT_RESOLVED = "CONFLICT_RESOLVED"
    AUTOFIX_APPLIED = "AUTOFIX_APPLIED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    ESCALATION_OPENED = "ESCALATION_OPENED"
    HUMAN_RESOLVED = "HUMAN_RESOLVED"
    RULE_CREATED = "RULE_CREATED"
    RULE_APPLIED = "RULE_APPLIED"
    PUSH_ATTEMPT = "PUSH_ATTEMPT"
    PUSH_SUCCEEDED = "PUSH_SUCCEEDED"
    PUSH_RETRIED = "PUSH_RETRIED"
    PUSH_FAILED = "PUSH_FAILED"
    BATCH_ROLLED_BACK = "BATCH_ROLLED_BACK"
    CIRCUIT_BREAKER_TRIPPED = "CIRCUIT_BREAKER_TRIPPED"
    RUN_COMPLETED = "RUN_COMPLETED"


class Actor:
    AGENT = "agent"
    RULE = "rule"
    HUMAN = "human"
    SYSTEM = "system"


def emit(
    conn: sqlite3.Connection,
    run_id: str,
    stage: str,
    event_type: str,
    actor: str,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    field: Optional[str] = None,
    before: Optional[Any] = None,
    after: Optional[Any] = None,
    reason: Optional[str] = None,
    score: Optional[float] = None,
    escalation_id: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> int:
    """Append a new event to the events log. Returns the new event's ID."""
    ts = datetime.now(timezone.utc).isoformat()
    meta_json = json.dumps(meta) if meta is not None else None
    before_str = json.dumps(before) if isinstance(before, (dict, list)) else (str(before) if before is not None else None)
    after_str = json.dumps(after) if isinstance(after, (dict, list)) else (str(after) if after is not None else None)

    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO events (
            run_id, ts, stage, type, actor, entity_type, entity_id, field,
            before, after, reason, score, escalation_id, meta_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id, ts, stage, event_type, actor, entity_type, entity_id, field,
            before_str, after_str, reason, score, escalation_id, meta_json
        ),
    )
    conn.commit()
    return cursor.lastrowid


def tail_events(conn: sqlite3.Connection, run_id: str, last_id: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
    """Tail events for a run after last_id."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM events
        WHERE run_id = ? AND id > ?
        ORDER BY id ASC
        LIMIT ?
        """,
        (run_id, last_id, limit),
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]
