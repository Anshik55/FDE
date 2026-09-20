"""Validation engine: implements validate -> auto-fix -> re-validate -> escalate ladder."""

from datetime import datetime, timezone
import re
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from app.events import Actor, EventType, emit
from app.pipeline.escalation import open_escalation
from app.pipeline.schema_loader import TargetSchema


def validate_single_record(
    record_data: Dict[str, Any],
    target_schema: TargetSchema,
) -> List[str]:
    """Runs schema validation rules on a record. Returns list of error messages."""
    errors = []

    # 1. Required field checks
    for field_name, fdef in target_schema.fields.items():
        val = str(record_data.get(field_name, "")).strip()
        if fdef.required and not val:
            errors.append(f"Missing required field: '{field_name}'")

    # 2. Email format check
    email_val = str(record_data.get("email", "")).strip()
    if email_val:
        if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email_val):
            errors.append(f"Invalid email format: '{email_val}'")

    # 3. Pattern checks (e.g. employee_id pattern)
    for field_name, fdef in target_schema.fields.items():
        if fdef.pattern:
            val = str(record_data.get(field_name, "")).strip()
            if val and not re.match(fdef.pattern, val):
                errors.append(f"Field '{field_name}' value '{val}' violates pattern {fdef.pattern}")

    # 4. Salary check
    salary_str = str(record_data.get("salary", "")).strip()
    if salary_str:
        try:
            sal_num = float(salary_str.replace(",", "").replace("$", ""))
            if sal_num < 0:
                errors.append(f"Salary must be non-negative, got {sal_num}")
        except ValueError:
            errors.append(f"Salary is not a valid number: '{salary_str}'")

    # 5. Enum validation
    for field_name, fdef in target_schema.fields.items():
        if fdef.type == "enum" and fdef.values:
            val = str(record_data.get(field_name, "")).strip()
            if val and val not in fdef.values:
                errors.append(f"Invalid enum value '{val}' for field '{field_name}'. Must be one of: {list(fdef.values.keys())}")

    return errors


def auto_fix_record(
    record_data: Dict[str, Any],
    errors: List[str],
) -> Tuple[Dict[str, Any], List[str]]:
    """Safe-fix ladder: attempts deterministic automatic repairs for common validation failures."""
    fixed = dict(record_data)
    applied_fixes = []

    for err in errors:
        # Auto-fix: email whitespace / uppercase
        if "email format" in err.lower() or "missing required field: 'email'" in err.lower():
            raw_email = str(fixed.get("email", "")).strip().lower()
            if " " in raw_email:
                cleaned_email = raw_email.replace(" ", "")
                if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", cleaned_email):
                    fixed["email"] = cleaned_email
                    applied_fixes.append("Removed whitespace from email format")

        # Auto-fix: salary comma/currency removal
        if "salary" in err.lower():
            sal_str = str(fixed.get("salary", "")).strip()
            sal_clean = re.sub(r"[^\d.]", "", sal_str)
            if sal_clean:
                try:
                    fixed["salary"] = str(float(sal_clean))
                    applied_fixes.append("Normalized salary currency and commas")
                except ValueError:
                    pass

    return fixed, applied_fixes


def validate_records(
    conn: sqlite3.Connection,
    run_id: str,
    canonical_records: List[Dict[str, Any]],
    target_schema: TargetSchema,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Runs the full Validate -> Auto-Fix -> Re-Validate -> Escalate loop.
    
    Returns:
        (valid_records, escalations_opened)
    """
    valid_records = []
    escalations = []

    for item in canonical_records:
        key = item["canonical_key"]
        data = item["data"]

        # Step 1: Initial validation
        errors_pass1 = validate_single_record(data, target_schema)

        if not errors_pass1:
            valid_records.append(item)
            continue

        # Step 2: Auto-fix attempt
        fixed_data, applied_fixes = auto_fix_record(data, errors_pass1)

        if applied_fixes:
            emit(
                conn=conn,
                run_id=run_id,
                stage="validate",
                event_type=EventType.AUTOFIX_APPLIED,
                actor=Actor.AGENT,
                entity_type="record",
                entity_id=key,
                reason=f"Applied autofix for {key}: {', '.join(applied_fixes)}",
                meta={"fixes": applied_fixes},
            )

        # Step 3: Re-validate after auto-fix
        errors_pass2 = validate_single_record(fixed_data, target_schema)

        if not errors_pass2:
            item["data"] = fixed_data
            valid_records.append(item)
        else:
            # Step 4: Fails validation twice -> ESCALATE!
            emit(
                conn=conn,
                run_id=run_id,
                stage="validate",
                event_type=EventType.VALIDATION_FAILED,
                actor=Actor.AGENT,
                entity_type="record",
                entity_id=key,
                reason=f"Record {key} failed validation twice: {'; '.join(errors_pass2)}",
                meta={"errors": errors_pass2},
            )

            esc_id = open_escalation(
                conn=conn,
                run_id=run_id,
                escalation_type="VALIDATION_FAILED_TWICE",
                group_key=f"val_fail_{key}",
                context={
                    "title": f"Record {key} Failed Validation Twice",
                    "entity_key": key,
                    "errors": errors_pass2,
                    "record_data": fixed_data,
                    "sample_values": [f"Error: {e}" for e in errors_pass2],
                },
                proposal={"action": "correct_and_retry", "rationale": "Manual correction required for missing required field or schema violation."},
                affected_ids=[key],
            )
            escalations.append({"key": key, "errors": errors_pass2, "escalation_id": esc_id})

    return valid_records, escalations
