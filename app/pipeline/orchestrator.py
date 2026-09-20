"""Pipeline orchestrator: coordinates end-to-end data migration with defensible escalation boundaries."""

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import uuid
from typing import Any, Dict, List, Optional
import pandas as pd

from app.db import get_db_connection, init_db
from app.events import Actor, EventType, emit
from app.pipeline.escalation import check_circuit_breaker, open_escalation
from app.pipeline.ingest import ingest_files
from app.pipeline.llm import LLMClient
from app.pipeline.mapping import propose_mappings
from app.pipeline.normalize import (
    normalize_casing_and_whitespace,
    normalize_enum_value,
    normalize_phone,
    parse_date_to_iso,
    resolve_column_date_format,
    split_composite_name,
)
from app.pipeline.profile import profile_columns
from app.pipeline.push import push_records
from app.pipeline.reconcile import reconcile_records
from app.pipeline.schema_loader import TargetSchema
from app.pipeline.validate import validate_records

BASE_DIR = Path(__file__).resolve().parent.parent.parent


def run_pipeline(
    file_paths: Optional[List[Path | str]] = None,
    client_id: str = "acme_corp",
    schema_path: Optional[Path | str] = None,
    push_to_target: bool = True,
    target_api_url: str = "http://localhost:8000/stub",
    conn: Optional[sqlite3.Connection] = None,
    pause_on_escalation: bool = False,
) -> Dict[str, Any]:
    """Execute end-to-end data migration pipeline."""
    db_conn = conn or init_db()
    run_id = f"run_{uuid.uuid4().hex[:8]}"
    started_at = datetime.now(timezone.utc).isoformat()

    target_schema_path = schema_path or (BASE_DIR / "schema" / "target_employee.yaml")
    target_schema = TargetSchema.from_yaml(target_schema_path)

    default_files = [
        BASE_DIR / "fixtures" / "hris_export.csv",
        BASE_DIR / "fixtures" / "payroll_export.xlsx",
        BASE_DIR / "fixtures" / "legacy_crm.csv",
    ]
    files_to_process = file_paths or default_files

    # 1. Start Run
    with db_conn:
        db_conn.execute(
            """
            INSERT INTO runs (id, client_id, status, started_at, config_json)
            VALUES (?, ?, 'running', ?, ?)
            """,
            (run_id, client_id, started_at, json.dumps({"files": [str(f) for f in files_to_process]})),
        )

    emit(
        conn=db_conn,
        run_id=run_id,
        stage="init",
        event_type=EventType.RUN_STARTED,
        actor=Actor.AGENT,
        reason=f"Started migration run {run_id} for client '{client_id}' with {len(files_to_process)} source files",
        meta={"files": [str(f) for f in files_to_process]},
    )

    # 2. Ingest
    datasets = ingest_files(db_conn, run_id, files_to_process)
    total_source_rows = sum(len(df) for df in datasets.values())

    # 3. Profile
    profiles = profile_columns(db_conn, run_id, datasets)

    # 4. Map
    llm = LLMClient(conn=db_conn)
    accepted_mappings, ambiguous_escalations = propose_mappings(
        conn=db_conn,
        run_id=run_id,
        profiles=profiles,
        target_schema=target_schema,
        client_id=client_id,
        llm_client=llm,
    )

    # Open escalations for ambiguous column mappings and sensitive confirmations
    for amb in ambiguous_escalations:
        esc_type = amb.get("type", "MAPPING_AMBIGUITY")
        if esc_type == "CONFIRM_SENSITIVE_MAPPINGS":
            open_escalation(
                conn=db_conn,
                run_id=run_id,
                escalation_type=esc_type,
                group_key=f"sensitive_mappings_{client_id}",
                context={
                    "title": "Confirm Sensitive Field Mappings (Salary, Date of Birth)",
                    "file": "all_sources",
                    "column": "sensitive_fields",
                    "sample_values": amb["sample_values"],
                },
                proposal={
                    "action": "approve_sensitive_mappings",
                    "confidence": 1.0,
                    "rationale": amb.get("llm_rationale", "One-click approval required for sensitive fields before migration."),
                },
                affected_ids=amb["sample_values"],
            )
        else:
            top1 = amb["top_candidates"][0]
            top2 = amb["top_candidates"][1] if len(amb["top_candidates"]) > 1 else None
            open_escalation(
                conn=db_conn,
                run_id=run_id,
                escalation_type="MAPPING_AMBIGUITY",
                group_key=f"mapping_{amb['file']}_{amb['column']}",
                context={
                    "title": f"Column '{amb['column']}' in {amb['file']}: {top1['field']} or {top2['field'] if top2 else 'unknown'}?",
                    "file": amb["file"],
                    "column": amb["column"],
                    "sample_values": amb["sample_values"],
                    "top_candidates": amb["top_candidates"],
                    "margin": amb["margin"],
                },
                proposal={
                    "target_field": top1["field"],
                    "confidence": top1["score"],
                    "rationale": amb.get("llm_rationale", ""),
                },
                affected_ids=[f"{amb['file']}:{amb['column']}"],
            )

    # 5. Normalize and Stage Records
    staged_records = []
    enum_unrecognized_groups: Dict[str, List[str]] = {}

    for filename, df in datasets.items():
        mapping = accepted_mappings.get(filename, {})
        if not mapping:
            continue

        # Check date formats for date columns in this file
        date_formats: Dict[str, Optional[str]] = {}
        for src_col, target_field in mapping.items():
            fdef = target_schema.fields.get(target_field)
            if fdef and fdef.type == "date":
                # Check learned date format rule from resolution memory
                date_rule = db_conn.execute(
                    "SELECT id, action_json FROM rules WHERE client_id = ? AND kind = 'date_format' AND fingerprint = ? AND enabled = 1",
                    (client_id, f"{filename}:{src_col}"),
                ).fetchone()
                if date_rule:
                    act = json.loads(date_rule["action_json"])
                    date_formats[src_col] = act.get("format", "%d/%m/%Y")
                    db_conn.execute("UPDATE rules SET times_applied = times_applied + 1 WHERE id = ?", (date_rule["id"],))
                    emit(
                        conn=db_conn,
                        run_id=run_id,
                        stage="normalize",
                        event_type=EventType.RULE_APPLIED,
                        actor=Actor.RULE,
                        field=src_col,
                        after=date_formats[src_col],
                        reason=f"Applied learned date format rule for '{src_col}' ({date_formats[src_col]})",
                    )
                    continue

                fmt, is_ambiguous, sample_vals = resolve_column_date_format(
                    series=df[src_col],
                    col_name=src_col,
                    filename=filename,
                    conn=db_conn,
                    run_id=run_id,
                )
                date_formats[src_col] = fmt
                if is_ambiguous:
                    # Genuinely ambiguous date format -> Escalate once per column!
                    open_escalation(
                        conn=db_conn,
                        run_id=run_id,
                        escalation_type="DATE_FORMAT_AMBIGUOUS",
                        group_key=f"date_fmt_{filename}_{src_col}",
                        context={
                            "title": f"Ambiguous Date Format in {filename} ('{src_col}')",
                            "file": filename,
                            "column": src_col,
                            "sample_values": sample_vals,
                            "options": ["DD/MM/YYYY", "MM/DD/YYYY"],
                        },
                        proposal={
                            "action": "choose_format",
                            "suggested_format": "DD/MM/YYYY",
                            "rationale": "Defaulting to DD/MM/YYYY based on regional standard; verify format.",
                        },
                        affected_ids=[f"{filename}:{src_col}"],
                    )

        # Process each row
        for idx, row in df.iterrows():
            rec_data = {}
            for src_col, target_field in mapping.items():
                val = str(row.get(src_col, "")).strip()
                if not val or val.lower() in ["nan", "none", "null"]:
                    continue

                if target_field == "full_name":
                    fn, ln = split_composite_name(val)
                    rec_data["first_name"] = fn
                    rec_data["last_name"] = ln
                    continue

                fdef = target_schema.fields.get(target_field)

                # Whitespace & Casing
                if fdef and fdef.type == "string" and ("name" in target_field or "title" in target_field):
                    val = normalize_casing_and_whitespace(val, is_name=("name" in target_field))
                elif fdef and fdef.type == "email":
                    val = val.lower().replace(" ", "")
                elif fdef and fdef.type == "phone":
                    val = normalize_phone(val)
                elif fdef and fdef.type == "date":
                    val = parse_date_to_iso(val, fmt=date_formats.get(src_col))
                elif fdef and fdef.type == "enum":
                    canon_val, recognized = normalize_enum_value(val, target_field, target_schema)
                    if recognized:
                        val = canon_val
                    else:
                        # Unrecognized enum (e.g. "LOA")
                        # Check if a learned rule exists
                        rule_row = db_conn.execute(
                            "SELECT action_json FROM rules WHERE client_id = ? AND kind = 'enum_value' AND fingerprint = ? AND enabled = 1",
                            (client_id, f"{target_field}:{val.upper()}"),
                        ).fetchone()
                        if rule_row:
                            act = json.loads(rule_row["action_json"])
                            val = act.get("canonical_value", canon_val)
                            emit(
                                conn=db_conn,
                                run_id=run_id,
                                stage="normalize",
                                event_type=EventType.RULE_APPLIED,
                                actor=Actor.RULE,
                                field=target_field,
                                before=canon_val,
                                after=val,
                                reason=f"Applied client resolution rule for enum '{canon_val}' → '{val}'",
                            )
                        else:
                            grp_k = f"{target_field}:{val.upper()}"
                            enum_unrecognized_groups.setdefault(grp_k, []).append(f"{filename}:row_{idx+1}")

                rec_data[target_field] = val

            staged_records.append({
                "file": filename,
                "row_num": int(idx) + 1,
                "data": rec_data,
            })

    # Open grouped escalations for unrecognized enums (e.g. "LOA")
    for grp_k, affected_rows in enum_unrecognized_groups.items():
        field_name, raw_enum = grp_k.split(":")
        fdef = target_schema.fields.get(field_name)
        canonical_choices = list(fdef.values.keys()) if fdef else []
        prop = llm.propose_enum_value(field_name, raw_enum, canonical_choices, run_id=run_id)

        open_escalation(
            conn=db_conn,
            run_id=run_id,
            escalation_type="ENUM_VALUE_AMBIGUOUS",
            group_key=f"enum_{grp_k}",
            context={
                "title": f"Unrecognized '{field_name}' Value: '{raw_enum}'",
                "field": field_name,
                "raw_value": raw_enum,
                "sample_values": [raw_enum],
                "valid_choices": canonical_choices,
            },
            proposal={
                "canonical_value": prop.get("canonical_value"),
                "confidence": prop.get("confidence", 0.8),
                "rationale": prop.get("rationale", ""),
            },
            affected_ids=affected_rows,
        )

    # 6. Reconcile across files
    canonical_records, reconcile_escalations = reconcile_records(
        conn=db_conn,
        run_id=run_id,
        staged_records=staged_records,
        target_schema=target_schema,
    )

    # 7. Validate records (Validate -> Auto-fix -> Re-validate -> Escalate)
    valid_records, validation_escalations = validate_records(
        conn=db_conn,
        run_id=run_id,
        canonical_records=canonical_records,
        target_schema=target_schema,
    )

    # Check volume circuit breaker
    check_circuit_breaker(
        conn=db_conn,
        run_id=run_id,
        total_source_rows=total_source_rows,
    )

    # Save ready canonical records to database
    with db_conn:
        for item in valid_records:
            emp_id = item["canonical_key"].replace("id:", "").replace("email:", "")
            can_id = f"can_{emp_id}"
            db_conn.execute(
                """
                INSERT OR REPLACE INTO canonical_records (
                    id, run_id, canonical_key, data_json, provenance_json, status
                ) VALUES (?, ?, ?, ?, ?, 'ready')
                """,
                (
                    can_id,
                    run_id,
                    emp_id,
                    json.dumps(item["data"]),
                    json.dumps(item.get("provenance", {})),
                ),
            )

    # Check if human review is needed before pushing to target
    pending_esc_count = db_conn.execute(
        "SELECT COUNT(*) as cnt FROM escalations WHERE run_id = ? AND status = 'pending'", (run_id,)
    ).fetchone()["cnt"]

    if pause_on_escalation and pending_esc_count > 0:
        with db_conn:
            db_conn.execute(
                "UPDATE runs SET status = 'paused' WHERE id = ?",
                (run_id,),
            )
        emit(
            conn=db_conn,
            run_id=run_id,
            stage="escalation",
            event_type="PIPELINE_PAUSED",
            actor=Actor.AGENT,
            reason=f"Pipeline paused: {pending_esc_count} decisions require human review before pushing to target API",
            meta={"pending_escalations": pending_esc_count, "ready_records": len(valid_records)},
        )
        return {
            "run_id": run_id,
            "status": "paused",
            "total_source_rows": total_source_rows,
            "canonical_records": len(canonical_records),
            "valid_records": len(valid_records),
            "records_processed": len(valid_records),
            "escalations_opened": pending_esc_count,
            "push_results": {"pushed_count": 0, "failed_count": 0},
        }

    # 8. Push ready records to target API
    push_results = {"pushed_count": 0, "failed_count": 0}
    if push_to_target and valid_records:
        try:
            push_results = push_records(
                conn=db_conn,
                run_id=run_id,
                records=valid_records,
                target_api_url=target_api_url,
            )
        except Exception as ex:
            emit(
                conn=db_conn,
                run_id=run_id,
                stage="push",
                event_type=EventType.PUSH_FAILED,
                actor=Actor.AGENT,
                reason=f"Push stage failed: {ex}",
            )

    # 9. Check if any escalations remain pending (e.g. from push rejections)
    pending_esc_count = db_conn.execute(
        "SELECT COUNT(*) as cnt FROM escalations WHERE run_id = ? AND status = 'pending'", (run_id,)
    ).fetchone()["cnt"]

    if pause_on_escalation and pending_esc_count > 0:
        with db_conn:
            db_conn.execute(
                "UPDATE runs SET status = 'paused' WHERE id = ?",
                (run_id,),
            )
        emit(
            conn=db_conn,
            run_id=run_id,
            stage="push",
            event_type="PIPELINE_PAUSED",
            actor=Actor.AGENT,
            reason=f"Pipeline paused: {pending_esc_count} item(s) require human review (Target API rejected {push_results.get('failed_count', 0)} records)",
            meta={"pending_escalations": pending_esc_count, "push": push_results},
        )
        return {
            "run_id": run_id,
            "status": "paused",
            "total_source_rows": total_source_rows,
            "canonical_records": len(canonical_records),
            "valid_records": len(valid_records),
            "records_processed": len(valid_records),
            "escalations_opened": pending_esc_count,
            "push_results": push_results,
        }

    # 10. Complete Run (only if 0 pending escalations)
    finished_at = datetime.now(timezone.utc).isoformat()
    with db_conn:
        db_conn.execute(
            "UPDATE runs SET status = 'completed', finished_at = ? WHERE id = ?",
            (finished_at, run_id),
        )

    emit(
        conn=db_conn,
        run_id=run_id,
        stage="complete",
        event_type=EventType.RUN_COMPLETED,
        actor=Actor.AGENT,
        reason=f"Run {run_id} completed: {len(valid_records)} canonical records ready, {push_results.get('pushed_count', 0)} pushed to target",
        meta={
            "canonical_count": len(canonical_records),
            "valid_count": len(valid_records),
            "push": push_results,
        },
    )

    # Count total open escalations
    esc_count = db_conn.execute(
        "SELECT COUNT(*) as cnt FROM escalations WHERE run_id = ?", (run_id,)
    ).fetchone()["cnt"]

    return {
        "run_id": run_id,
        "status": "completed",
        "total_source_rows": total_source_rows,
        "canonical_records": len(canonical_records),
        "valid_records": len(valid_records),
        "records_processed": len(valid_records),
        "escalations_opened": esc_count,
        "push_results": push_results,
    }


def resume_pipeline(
    run_id: str,
    conn: Optional[sqlite3.Connection] = None,
    push_to_target: bool = True,
    target_api_url: str = "http://localhost:8000/stub",
) -> Dict[str, Any]:
    """Resume a paused pipeline run and push ready records to target API."""
    db_conn = conn or init_db()

    emit(
        conn=db_conn,
        run_id=run_id,
        stage="push",
        event_type="PIPELINE_RESUMED",
        actor=Actor.HUMAN,
        reason=f"Human reviewer resumed pipeline execution for run {run_id}",
    )

    # Fetch ready records from canonical_records table
    rows = db_conn.execute(
        "SELECT canonical_key, data_json, provenance_json FROM canonical_records WHERE run_id = ? AND status IN ('ready', 'pending')",
        (run_id,),
    ).fetchall()

    valid_records = [
        {
            "canonical_key": r["canonical_key"],
            "data": json.loads(r["data_json"]),
            "provenance": json.loads(r["provenance_json"]),
        }
        for r in rows
    ]

    push_results = {"pushed_count": 0, "failed_count": 0}
    if push_to_target and valid_records:
        try:
            push_results = push_records(
                conn=db_conn,
                run_id=run_id,
                records=valid_records,
                target_api_url=target_api_url,
            )
        except Exception as ex:
            emit(
                conn=db_conn,
                run_id=run_id,
                stage="push",
                event_type=EventType.PUSH_FAILED,
                actor=Actor.AGENT,
                reason=f"Push stage failed on resume: {ex}",
            )

    # Check if any escalations remain pending (e.g. from 4xx target push rejections)
    pending_esc_count = db_conn.execute(
        "SELECT COUNT(*) as cnt FROM escalations WHERE run_id = ? AND status = 'pending'", (run_id,)
    ).fetchone()["cnt"]

    if pending_esc_count > 0:
        with db_conn:
            db_conn.execute(
                "UPDATE runs SET status = 'paused' WHERE id = ?",
                (run_id,),
            )
        emit(
            conn=db_conn,
            run_id=run_id,
            stage="push",
            event_type="PIPELINE_PAUSED",
            actor=Actor.AGENT,
            reason=f"Pipeline paused: {pending_esc_count} item(s) rejected by Target API require human review before completion",
            meta={"pending_escalations": pending_esc_count, "push": push_results},
        )
        return {
            "run_id": run_id,
            "status": "paused",
            "valid_records": len(valid_records),
            "escalations_opened": pending_esc_count,
            "push_results": push_results,
        }

    finished_at = datetime.now(timezone.utc).isoformat()
    with db_conn:
        db_conn.execute(
            "UPDATE runs SET status = 'completed', finished_at = ? WHERE id = ?",
            (finished_at, run_id),
        )

    emit(
        conn=db_conn,
        run_id=run_id,
        stage="complete",
        event_type=EventType.RUN_COMPLETED,
        actor=Actor.AGENT,
        reason=f"Run {run_id} resumed & completed: {push_results.get('pushed_count', 0)} pushed to target",
        meta={
            "valid_count": len(valid_records),
            "push": push_results,
        },
    )

    return {
        "run_id": run_id,
        "status": "completed",
        "valid_records": len(valid_records),
        "push_results": push_results,
    }
