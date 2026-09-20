"""Resolution memory: stores and applies learned rules from human resolutions."""

from datetime import datetime, timezone
import json
import sqlite3
import uuid
from typing import Any, Dict, List, Optional

from app.events import Actor, EventType, emit


def create_or_update_rule(
    conn: sqlite3.Connection,
    client_id: str,
    kind: str,
    fingerprint: str,
    action: Dict[str, Any],
    source_escalation_id: Optional[str] = None,
    run_id: Optional[str] = None,
) -> str:
    """Creates or updates a persistent client-scoped rule."""
    rule_id = f"rule_{uuid.uuid4().hex[:8]}"
    ts = datetime.now(timezone.utc).isoformat()
    action_json = json.dumps(action)

    with conn:
        conn.execute(
            """
            INSERT INTO rules (
                id, client_id, kind, fingerprint, action_json,
                source_escalation_id, enabled, created_at, times_applied
            ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, 0)
            ON CONFLICT(client_id, kind, fingerprint) DO UPDATE SET
                action_json = excluded.action_json,
                source_escalation_id = excluded.source_escalation_id,
                enabled = 1
            """,
            (rule_id, client_id, kind, fingerprint, action_json, source_escalation_id, ts),
        )

    # Determine valid run_id for FK
    effective_run_id = run_id
    if not effective_run_id:
        row = conn.execute("SELECT id FROM runs ORDER BY started_at DESC LIMIT 1").fetchone()
        effective_run_id = row["id"] if row else None

    if effective_run_id:
        emit(
            conn=conn,
            run_id=effective_run_id,
            stage="rules",
            event_type=EventType.RULE_CREATED,
            actor=Actor.HUMAN,
            reason=f"Synthesized persistent {kind} rule for client '{client_id}': {fingerprint} → {action}",
            meta={"client_id": client_id, "kind": kind, "fingerprint": fingerprint, "action": action},
        )

    return rule_id


def find_rule(
    conn: sqlite3.Connection,
    client_id: str,
    kind: str,
    fingerprint: str,
) -> Optional[Dict[str, Any]]:
    """Look up an active rule for a client."""
    row = conn.execute(
        """
        SELECT * FROM rules
        WHERE client_id = ? AND kind = ? AND fingerprint = ? AND enabled = 1
        """,
        (client_id, kind, fingerprint),
    ).fetchone()

    if row:
        return {
            "id": row["id"],
            "client_id": row["client_id"],
            "kind": row["kind"],
            "fingerprint": row["fingerprint"],
            "action": json.loads(row["action_json"]),
            "times_applied": row["times_applied"],
        }
    return None


def record_rule_applied(
    conn: sqlite3.Connection,
    run_id: str,
    rule_id: str,
    stage: str,
    field: Optional[str] = None,
    before: Optional[Any] = None,
    after: Optional[Any] = None,
    reason: Optional[str] = None,
):
    """Increment times_applied and emit RULE_APPLIED event."""
    with conn:
        conn.execute(
            "UPDATE rules SET times_applied = times_applied + 1 WHERE id = ?",
            (rule_id,),
        )

    emit(
        conn=conn,
        run_id=run_id,
        stage=stage,
        event_type=EventType.RULE_APPLIED,
        actor=Actor.RULE,
        field=field,
        before=before,
        after=after,
        reason=reason or f"Applied learned rule {rule_id}",
    )


def synthesize_rule_from_resolution(
    conn: sqlite3.Connection,
    client_id: str,
    escalation_row: sqlite3.Row,
    action: str,
    value: Optional[Any] = None,
) -> Optional[str]:
    """Translates a human resolution into a persistent rule."""
    esc_type = escalation_row["type"]
    context = json.loads(escalation_row["context_json"])

    if esc_type == "CONFIRM_SENSITIVE_MAPPINGS" and action == "approve":
        return create_or_update_rule(
            conn=conn,
            client_id=client_id,
            kind="sensitive_mapping_confirmed",
            fingerprint="all_sensitive",
            action={"action": "approved"},
            source_escalation_id=escalation_row["id"],
        )

    elif esc_type == "ENUM_VALUE_AMBIGUOUS" and action in ["approve", "correct"]:
        raw_val = context.get("raw_value")
        field_name = context.get("field")
        chosen_val = value if action == "correct" and value else json.loads(escalation_row["proposal_json"] or "{}").get("canonical_value")
        if raw_val and field_name and chosen_val:
            return create_or_update_rule(
                conn=conn,
                client_id=client_id,
                kind="enum_value",
                fingerprint=f"{field_name}:{raw_val.upper()}",
                action={"canonical_value": chosen_val},
                source_escalation_id=escalation_row["id"],
            )

    elif esc_type == "MAPPING_AMBIGUITY" and action in ["approve", "correct"]:
        file_name = context.get("file")
        col_name = context.get("column")
        chosen_field = value if action == "correct" and value else json.loads(escalation_row["proposal_json"] or "{}").get("target_field")
        if file_name and col_name and chosen_field:
            return create_or_update_rule(
                conn=conn,
                client_id=client_id,
                kind="mapping",
                fingerprint=f"{file_name}:{col_name}",
                action={"target_field": chosen_field},
                source_escalation_id=escalation_row["id"],
            )

    elif esc_type == "DATE_FORMAT_AMBIGUOUS" and action in ["approve", "correct"]:
        file_name = context.get("file")
        col_name = context.get("column")
        chosen_fmt = value if action == "correct" and value else "%d/%m/%Y"
        if file_name and col_name:
            return create_or_update_rule(
                conn=conn,
                client_id=client_id,
                kind="date_format",
                fingerprint=f"{file_name}:{col_name}",
                action={"format": chosen_fmt},
                source_escalation_id=escalation_row["id"],
            )

    return None
