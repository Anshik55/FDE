"""Multi-file reconciliation: entity matching, exact merging, conflict resolution, and near-duplicate detection."""

import copy
import json
import sqlite3
from typing import Any, Dict, List, Optional, Tuple
from rapidfuzz import fuzz

from app.events import Actor, EventType, emit
from app.pipeline.escalation import open_escalation
from app.pipeline.schema_loader import TargetSchema


def build_match_key(record: Dict[str, Any]) -> str:
    """Derive primary match key: employee_id -> email -> (name + dob)."""
    emp_id = record.get("employee_id")
    if emp_id and str(emp_id).strip():
        return f"id:{str(emp_id).strip()}"

    email = record.get("email")
    if email and str(email).strip():
        return f"email:{str(email).strip().lower()}"

    fn = record.get("first_name", "").strip().lower()
    ln = record.get("last_name", "").strip().lower()
    dob = record.get("date_of_birth", "").strip()
    if fn and ln:
        return f"name:{fn}_{ln}_{dob}"

    return f"raw:{id(record)}"


def find_matching_key(
    data: Dict[str, Any],
    id_index: Dict[str, str],
    email_index: Dict[str, str],
    name_index: Dict[str, str],
) -> Optional[str]:
    """Find existing canonical key by employee_id -> email -> (name)."""
    emp_id = str(data.get("employee_id", "")).strip()
    if emp_id and emp_id in id_index:
        return id_index[emp_id]

    email = str(data.get("email", "")).strip().lower()
    if email and email in email_index:
        return email_index[email]

    fn = str(data.get("first_name", "")).strip().lower()
    ln = str(data.get("last_name", "")).strip().lower()
    if fn and ln:
        name_k = f"{fn}_{ln}"
        if name_k in name_index:
            return name_index[name_k]

    return None


def register_indexes(
    key: str,
    data: Dict[str, Any],
    id_index: Dict[str, str],
    email_index: Dict[str, str],
    name_index: Dict[str, str],
):
    emp_id = str(data.get("employee_id", "")).strip()
    if emp_id:
        id_index[emp_id] = key

    email = str(data.get("email", "")).strip().lower()
    if email:
        email_index[email] = key

    fn = str(data.get("first_name", "")).strip().lower()
    ln = str(data.get("last_name", "")).strip().lower()
    if fn and ln:
        name_index[f"{fn}_{ln}"] = key


def reconcile_records(
    conn: sqlite3.Connection,
    run_id: str,
    staged_records: List[Dict[str, Any]],  # list of {file, raw_row, data}
    target_schema: TargetSchema,
    source_precedence: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Reconciles staged records across multiple source files into a unified dataset."""
    precedence = source_precedence or ["hris_export.csv", "legacy_crm.csv", "payroll_export.xlsx"]
    precedence_rank = {filename: idx for idx, filename in enumerate(precedence)}

    canonical_map: Dict[str, Dict[str, Any]] = {}
    provenance_map: Dict[str, Dict[str, str]] = {}
    escalations: List[Dict[str, Any]] = []

    id_index: Dict[str, str] = {}
    email_index: Dict[str, str] = {}
    name_index: Dict[str, str] = {}

    # 1. Multi-Key Entity Linking & Merging
    for item in staged_records:
        src_file = item["file"]
        data = item["data"]

        matched_key = find_matching_key(data, id_index, email_index, name_index)

        if not matched_key:
            # New canonical entity
            key = build_match_key(data)
            canonical_map[key] = copy.deepcopy(data)
            provenance_map[key] = {f: src_file for f, val in data.items() if str(val).strip()}
            register_indexes(key, data, id_index, email_index, name_index)
        else:
            key = matched_key
            existing = canonical_map[key]
            emit(
                conn=conn,
                run_id=run_id,
                stage="reconcile",
                event_type=EventType.RECORDS_MERGED,
                actor=Actor.AGENT,
                entity_type="record",
                entity_id=key,
                reason=f"Merged record from {src_file} into existing entity {key}",
                meta={"source_file": src_file, "key": key},
            )

            for f_name, new_val in data.items():
                if not str(new_val).strip():
                    continue

                exist_val = existing.get(f_name, "")
                field_def = target_schema.fields.get(f_name)
                is_sensitive = field_def.sensitive if field_def else False

                if not str(exist_val).strip():
                    # Null-vs-value: backfill from secondary file!
                    existing[f_name] = new_val
                    provenance_map[key][f_name] = src_file
                elif str(exist_val).strip() != str(new_val).strip():
                    # True Conflict!
                    orig_file = provenance_map[key].get(f_name, "unknown")
                    if is_sensitive:
                        # Sensitive conflict (e.g. Salary, DOB) -> ESCALATE!
                        esc_id = open_escalation(
                            conn=conn,
                            run_id=run_id,
                            escalation_type="SENSITIVE_FIELD_CONFLICT",
                            group_key=f"conflict_{key}_{f_name}",
                            context={
                                "title": f"Sensitive Conflict on '{f_name}' for {key}",
                                "field": f_name,
                                "entity_key": key,
                                "source_1": {"file": orig_file, "value": exist_val},
                                "source_2": {"file": src_file, "value": new_val},
                                "sample_values": [f"{orig_file}: {exist_val}", f"{src_file}: {new_val}"],
                            },
                            proposal={"action": "choose_source", "suggested_source": orig_file, "suggested_value": exist_val},
                            affected_ids=[key],
                        )
                        escalations.append({"type": "SENSITIVE_FIELD_CONFLICT", "key": key, "field": f_name})
                    else:
                        # Non-sensitive conflict (e.g. Job Title) -> Source Precedence!
                        orig_rank = precedence_rank.get(orig_file, 99)
                        new_rank = precedence_rank.get(src_file, 99)
                        winning_file = orig_file if orig_rank <= new_rank else src_file
                        winning_val = exist_val if orig_rank <= new_rank else new_val

                        existing[f_name] = winning_val
                        provenance_map[key][f_name] = winning_file

                        emit(
                            conn=conn,
                            run_id=run_id,
                            stage="reconcile",
                            event_type=EventType.CONFLICT_RESOLVED,
                            actor=Actor.RULE,
                            entity_type="field",
                            entity_id=f"{key}:{f_name}",
                            field=f_name,
                            before=f"{orig_file}='{exist_val}' vs {src_file}='{new_val}'",
                            after=winning_val,
                            reason=f"Resolved '{f_name}' conflict via source precedence ({winning_file} won)",
                        )

    # 2. Near-Duplicate Detection (Planted Case 4: Rahul Sharma vs Rahul Sherma)
    canonical_list = list(canonical_map.items())
    for i in range(len(canonical_list)):
        key_i, rec_i = canonical_list[i]
        name_i = f"{rec_i.get('first_name', '')} {rec_i.get('last_name', '')}".strip()
        dob_i = rec_i.get("date_of_birth", "").strip()

        for j in range(i + 1, len(canonical_list)):
            key_j, rec_j = canonical_list[j]
            name_j = f"{rec_j.get('first_name', '')} {rec_j.get('last_name', '')}".strip()
            dob_j = rec_j.get("date_of_birth", "").strip()

            if name_i and name_j and dob_i and dob_j and dob_i == dob_j:
                similarity = fuzz.token_sort_ratio(name_i.lower(), name_j.lower())
                if similarity >= 85 and key_i != key_j:
                    # Near duplicate with same DOB -> Escalate!
                    esc_id = open_escalation(
                        conn=conn,
                        run_id=run_id,
                        escalation_type="NEAR_DUPLICATE_RECORD",
                        group_key=f"near_dup_{key_i}_{key_j}",
                        context={
                            "title": f"Near-Duplicate: '{name_i}' ({key_i}) vs '{name_j}' ({key_j})",
                            "record_a": rec_i,
                            "record_b": rec_j,
                            "similarity": round(similarity, 1),
                            "shared_dob": dob_i,
                            "sample_values": [f"Record A: {name_i}, email: {rec_i.get('email')}", f"Record B: {name_j}, email: {rec_j.get('email')}"],
                        },
                        proposal={"action": "review_merge_or_separate", "rationale": "High name similarity with identical birth date; verify if same person."},
                        affected_ids=[key_i, key_j],
                    )
                    escalations.append({"type": "NEAR_DUPLICATE_RECORD", "records": [key_i, key_j]})

    # Format result canonical records
    output_records = []
    for key, data in canonical_map.items():
        output_records.append({
            "canonical_key": key,
            "data": data,
            "provenance": provenance_map.get(key, {}),
        })

    return output_records, escalations
