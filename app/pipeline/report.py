"""Reconciliation report and autonomy scoreboard generator."""

import json
import sqlite3
from typing import Any, Dict, List, Optional


def generate_reconciliation_report(conn: sqlite3.Connection, run_id: Optional[str] = None) -> Dict[str, Any]:
    """Generates an executive reconciliation report and autonomy scoreboard."""
    cursor = conn.cursor()

    if not run_id:
        row = cursor.execute("SELECT id FROM runs ORDER BY started_at DESC LIMIT 1").fetchone()
        if not row:
            return {
                "run_id": "No Active Run",
                "summary": {
                    "source_rows_in": 0,
                    "canonical_records_merged": 0,
                    "pushed_to_target": 0,
                    "failed": 0,
                    "rejected_4xx": 0,
                    "rolled_back": 0,
                    "pending_review": 0,
                    "resolved_by_human": 0,
                },
                "autonomy": {
                    "score": 100.0,
                    "total_decisions": 0,
                    "auto_resolved": 0,
                    "human_escalated": 0,
                    "agent_actions": 0,
                    "rule_actions": 0,
                    "stage_breakdown": {},
                },
                "dropped_columns": [],
                "precedence_conflicts": [],
                "records": [],
            }
        run_id = row["id"]

    # 1. Total Source Rows
    source_count = cursor.execute(
        "SELECT COUNT(*) as cnt FROM source_rows WHERE run_id = ?", (run_id,)
    ).fetchone()["cnt"]

    # 2. Canonical Records stats
    rec_rows = cursor.execute(
        "SELECT * FROM canonical_records WHERE run_id = ?", (run_id,)
    ).fetchall()

    status_counts = {"ready": 0, "pushed": 0, "failed": 0, "rejected": 0, "rolled_back": 0}
    records = []
    precedence_conflicts = []

    for r in rec_rows:
        status = r["status"]
        status_counts[status] = status_counts.get(status, 0) + 1
        data = json.loads(r["data_json"])
        provenance = json.loads(r["provenance_json"])

        records.append({
            "canonical_key": r["canonical_key"],
            "name": f"{data.get('first_name', '')} {data.get('last_name', '')}".strip(),
            "email": data.get("email", ""),
            "department": data.get("department", ""),
            "status": status,
            "data": data,
            "provenance": provenance,
        })

    # 3. Autonomy Scoreboard from events table
    # Events grouped by actor
    actor_rows = cursor.execute(
        """
        SELECT actor, stage, COUNT(*) as cnt
        FROM events
        WHERE run_id = ? AND actor IN ('agent', 'rule', 'human')
        GROUP BY actor, stage
        """,
        (run_id,),
    ).fetchall()

    agent_count = 0
    rule_count = 0
    human_count = 0
    stage_breakdown: Dict[str, Dict[str, int]] = {}

    for row in actor_rows:
        actor = row["actor"]
        stage = row["stage"]
        cnt = row["cnt"]

        if stage not in stage_breakdown:
            stage_breakdown[stage] = {"agent": 0, "rule": 0, "human": 0}
        stage_breakdown[stage][actor] += cnt

        if actor == "agent":
            agent_count += cnt
        elif actor == "rule":
            rule_count += cnt
        elif actor == "human":
            human_count += cnt

    total_decisions = agent_count + rule_count + human_count
    auto_decisions = agent_count + rule_count
    autonomy_score = round((auto_decisions / total_decisions * 100), 1) if total_decisions > 0 else 100.0

    # 4. Dropped Columns (columns profiled with 0 target mapping or MAPPING_DROPPED event)
    dropped_events = cursor.execute(
        """
        SELECT meta_json, reason FROM events 
        WHERE run_id = ? AND type = 'COLUMN_PROFILED'
        """,
        (run_id,),
    ).fetchall()

    dropped_columns = []
    for ev in dropped_events:
        if ev["meta_json"]:
            meta = json.loads(ev["meta_json"])
            # If mapping score was low or dropped
            if meta.get("dropped"):
                dropped_columns.append({
                    "file": meta.get("file", "unknown"),
                    "column": meta.get("column", "unknown"),
                    "reason": ev["reason"],
                })

    # 5. Precedence Conflicts (from events where type = 'CONFLICT_RESOLVED')
    conflict_events = cursor.execute(
        """
        SELECT entity_id, field, before, after, reason, meta_json
        FROM events
        WHERE run_id = ? AND type = 'CONFLICT_RESOLVED'
        """,
        (run_id,),
    ).fetchall()

    for ce in conflict_events:
        meta = json.loads(ce["meta_json"]) if ce["meta_json"] else {}
        precedence_conflicts.append({
            "employee_id": ce["entity_id"],
            "field": ce["field"],
            "winning_source": meta.get("winner_source", "HRIS"),
            "winning_value": ce["after"],
            "losing_source": meta.get("loser_source", "CRM/Payroll"),
            "losing_value": ce["before"],
            "reason": ce["reason"],
        })

    # 6. Escalations summary
    esc_counts = cursor.execute(
        """
        SELECT status, COUNT(*) as cnt
        FROM escalations
        WHERE run_id = ?
        GROUP BY status
        """,
        (run_id,),
    ).fetchall()
    escalations_summary = {r["status"]: r["cnt"] for r in esc_counts}

    return {
        "run_id": run_id,
        "summary": {
            "source_rows_in": source_count,
            "canonical_records_merged": len(records),
            "pushed_to_target": status_counts.get("pushed", 0),
            "failed": status_counts.get("failed", 0),
            "rejected_4xx": status_counts.get("rejected", 0),
            "rolled_back": status_counts.get("rolled_back", 0),
            "pending_review": escalations_summary.get("pending", 0),
            "resolved_by_human": escalations_summary.get("resolved", 0),
        },
        "autonomy": {
            "score": autonomy_score,
            "total_decisions": total_decisions,
            "auto_resolved": auto_decisions,
            "human_escalated": human_count,
            "agent_actions": agent_count,
            "rule_actions": rule_count,
            "stage_breakdown": stage_breakdown,
        },
        "dropped_columns": dropped_columns,
        "precedence_conflicts": precedence_conflicts,
        "records": records,
    }
