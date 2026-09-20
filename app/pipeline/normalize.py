"""Data normalization stage: whitespace, casing, phone (E.164), date format resolution, and enums."""

from datetime import datetime, timezone
import re
import sqlite3
from typing import Any, Dict, List, Optional, Tuple
from dateutil import parser as date_parser
import pandas as pd
import phonenumbers

from app.events import Actor, EventType, emit
from app.pipeline.schema_loader import TargetSchema


def normalize_casing_and_whitespace(val: str, is_name: bool = False) -> str:
    """Clean whitespace; re-case only if ALL-CAPS or all-lowercase."""
    cleaned = re.sub(r"\s+", " ", str(val).strip())
    if not cleaned:
        return ""
    if is_name:
        if cleaned.isupper() or cleaned.islower():
            return cleaned.title()
    return cleaned


def normalize_phone(phone_str: str, default_region: str = "IN") -> str:
    """Normalize phone number to international E.164 format."""
    clean = re.sub(r"[^\d+]", "", str(phone_str))
    if not clean:
        return ""
    try:
        parsed = phonenumbers.parse(phone_str, default_region)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        pass
    return clean


def split_composite_name(full_name: str) -> Tuple[str, str]:
    """Split composite full name into first_name and last_name."""
    tokens = re.sub(r"\s+", " ", str(full_name).strip()).split(" ")
    if not tokens or not tokens[0]:
        return "", ""
    if len(tokens) == 1:
        return tokens[0].title(), ""
    return tokens[0].title(), " ".join(tokens[1:]).title()


def resolve_column_date_format(
    series: pd.Series,
    col_name: str,
    filename: str,
    conn: sqlite3.Connection,
    run_id: str,
) -> Tuple[Optional[str], bool, List[str]]:
    """Determine date format for a column.
    
    Returns:
        (detected_format, is_ambiguous, sample_values)
    """
    clean_vals = [str(v).strip() for v in series if str(v).strip() and str(v).lower() not in ["nan", "none", "null", ""]]
    if not clean_vals:
        return None, False, []

    # Check for values with day > 12
    has_day_gt_12 = False
    candidate_formats = ["%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d", "%d-%b-%Y"]
    surviving_formats = list(candidate_formats)

    for val in clean_vals:
        # Check standard slash formats
        parts = re.split(r"[-/]", val)
        if len(parts) >= 2:
            try:
                first = int(parts[0])
                if first > 12:
                    has_day_gt_12 = True
            except ValueError:
                pass

    if has_day_gt_12:
        # Unambiguous DD/MM
        emit(
            conn=conn,
            run_id=run_id,
            stage="normalize",
            event_type=EventType.DATE_FORMAT_RESOLVED,
            actor=Actor.AGENT,
            entity_type="column",
            entity_id=f"{filename}:{col_name}",
            field=col_name,
            after="DD/MM/YYYY",
            reason=f"Column '{col_name}' has values with day > 12 proving DD/MM format",
        )
        return "%d/%m/%Y", False, clean_vals[:5]

    # Check if format is already ISO YYYY-MM-DD
    if all(re.match(r"^\d{4}-\d{2}-\d{2}$", v) for v in clean_vals):
        return "%Y-%m-%d", False, clean_vals[:5]

    # If all values have first and second parts <= 12 -> Ambiguous!
    all_le_12 = True
    for val in clean_vals:
        parts = re.split(r"[-/]", val)
        if len(parts) >= 2:
            try:
                p1, p2 = int(parts[0]), int(parts[1])
                if p1 > 12 or p2 > 12:
                    all_le_12 = False
                    break
            except ValueError:
                all_le_12 = False
                break

    if all_le_12:
        return None, True, clean_vals[:5]

    return "%d/%m/%Y", False, clean_vals[:5]


def parse_date_to_iso(val: str, fmt: Optional[str] = None) -> str:
    """Parse date string to ISO YYYY-MM-DD."""
    val_str = str(val).strip()
    if not val_str:
        return ""
    if fmt:
        try:
            dt = datetime.strptime(val_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    try:
        dt = date_parser.parse(val_str, dayfirst=(fmt == "%d/%m/%Y"))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return val_str


def normalize_enum_value(
    raw_val: str,
    target_field: str,
    target_schema: TargetSchema,
) -> Tuple[str, bool]:
    """Map raw enum string to canonical value using synonym dicts.
    
    Returns:
        (canonical_value, is_recognized)
    """
    clean_val = str(raw_val).strip()
    if not clean_val:
        return "", True

    field_def = target_schema.fields.get(target_field)
    if not field_def or field_def.type != "enum":
        return clean_val, True

    lower_raw = clean_val.lower()

    # Exact canonical match
    for canon, syns in field_def.values.items():
        if lower_raw == canon.lower():
            return canon, True
        for syn in syns:
            if lower_raw == syn.lower():
                return canon, True

    # Unrecognized enum value (e.g. "LOA")
    return clean_val, False
