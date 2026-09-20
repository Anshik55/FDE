"""Escalation management: policy checks, grouping, circuit breaker, and resolution."""

from datetime import datetime, timezone
import json
import sqlite3
import uuid
from typing import Any, Dict, List, Optional

from app.events import Actor, EventType, emit


def open_escalation(
    conn: sqlite3.Connection,
    run_id: str,
    escalation_type: str,
    group_key: str,
    context: Dict[str, Any],
    proposal: Optional[Dict[str, Any]] = None,
    affected_ids: Optional[List[str]] = None,
) -> str:
    """Create or add to a first-class escalation item."""
    esc_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).isoformat()
    affected = affected_ids or []

    # Check if an open escalation with this group_key already exists in this run
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, affected_ids_json FROM escalations WHERE run_id = ? AND group_key = ? AND status = 'pending'",
        (run_id, group_key),
    )
    existing = cursor.fetchone()
    if existing:
        existing_id = existing["id"]
        existing_affected = json.loads(existing["affected_ids_json"] or "[]")
        combined_affected = list(set(existing_affected + affected))
        cursor.execute(
            "UPDATE escalations SET affected_ids_json = ? WHERE id = ?",
            (json.dumps(combined_affected), existing_id),
        )
        conn.commit()
        return existing_id

    cursor.execute(
        """
        INSERT INTO escalations (
            id, run_id, group_key, type, status, context_json,
            proposal_json, affected_ids_json, created_at
        ) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?)
        """,
        (
            esc_id,
            run_id,
            group_key,
            escalation_type,
            json.dumps(context),
            json.dumps(proposal) if proposal else None,
            json.dumps(affected),
            ts,
        ),
    )
    conn.commit()

    emit(
        conn=conn,
        run_id=run_id,
        stage="escalation",
        event_type=EventType.ESCALATION_OPENED,
        actor=Actor.AGENT,
        entity_type="escalation",
        entity_id=esc_id,
        escalation_id=esc_id,
        reason=f"Opened escalation '{escalation_type}': {context.get('title', group_key)} (affects {len(affected)} items)",
        meta={"group_key": group_key, "proposal": proposal},
    )

    return esc_id


def check_circuit_breaker(
    conn: sqlite3.Connection,
    run_id: str,
    total_source_rows: int,
    max_escalation_groups: int = 20,
    max_escalated_row_ratio: float = 0.30,
) -> bool:
    """Check if the volume circuit breaker should trip. Returns True if tripped."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(DISTINCT group_key) as group_count FROM escalations WHERE run_id = ?",
        (run_id,),
    )
    group_count = cursor.fetchone()["group_count"]

    if group_count > max_escalation_groups:
        emit(
            conn=conn,
            run_id=run_id,
            stage="escalation",
            event_type=EventType.CIRCUIT_BREAKER_TRIPPED,
            actor=Actor.SYSTEM,
            reason=f"Circuit breaker tripped: {group_count} escalation groups exceeded max threshold ({max_escalation_groups}). Run halted.",
        )
        return True

    return False


def resolve_escalation(
    conn: sqlite3.Connection,
    escalation_id: str,
    action: str,  # approve | correct | reject
    resolved_by: str = "human",
    value: Optional[Any] = None,
    remember: bool = True,
) -> Dict[str, Any]:
    """Resolve an open escalation and record the resolution."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM escalations WHERE id = ?", (escalation_id,))
    row = cursor.fetchone()
    if not row:
        raise ValueError(f"Escalation {escalation_id} not found")

    ts = datetime.now(timezone.utc).isoformat()
    resolution_data = {
        "action": action,
        "value": value,
        "remember": remember,
        "timestamp": ts,
    }

    cursor.execute(
        """
        UPDATE escalations
        SET status = 'resolved', resolved_at = ?, resolved_by = ?, resolution_json = ?
        WHERE id = ?
        """,
        (ts, resolved_by, json.dumps(resolution_data), escalation_id),
    )
    conn.commit()

    # If remember is requested, synthesize client-scoped rule
    rule_id = None
    if remember:
        from app.pipeline.rules import synthesize_rule_from_resolution
        run_row = conn.execute("SELECT client_id FROM runs WHERE id = ?", (row["run_id"],)).fetchone()
        client_id = run_row["client_id"] if run_row else "acme_corp"
        rule_id = synthesize_rule_from_resolution(conn, client_id, row, action, value)

    emit(
        conn=conn,
        run_id=row["run_id"],
        stage="escalation",
        event_type=EventType.HUMAN_RESOLVED,
        actor=Actor.HUMAN,
        entity_type="escalation",
        entity_id=escalation_id,
        escalation_id=escalation_id,
        before=json.loads(row["proposal_json"]) if row["proposal_json"] else None,
        after=resolution_data,
        reason=f"Human resolved escalation '{row['type']}' with action '{action}'{' (saved to memory)' if rule_id else ''}",
    )

    return {
        "escalation_id": escalation_id,
        "run_id": row["run_id"],
        "status": "resolved",
        "action": action,
        "value": value,
        "remember": remember,
    }
