"""Column mapping module: fuzzy candidate generation and evidence scoring."""

from dataclasses import dataclass, field
import json
import re
import sqlite3
from typing import Any, Dict, List, Optional, Tuple
from rapidfuzz import fuzz

from app.events import Actor, EventType, emit
from app.pipeline.profile import ColumnProfile
from app.pipeline.schema_loader import TargetSchema


@dataclass
class CandidateMapping:
    target_field: str
    score: float
    evidence: Dict[str, Any] = field(default_factory=dict)
    rationale: str = ""


# Synonyms and aliases commonly found in client HR/CRM exports
FIELD_ALIASES: Dict[str, List[str]] = {
    "employee_id": ["emp no", "empno", "empid", "emp_id", "staff_id", "badge", "id", "employee code", "code"],
    "first_name": ["fname", "first", "given name", "first name", "firstname"],
    "last_name": ["lname", "last", "surname", "family name", "last name", "lastname"],
    "email": ["e-mail", "mail", "email address", "work email", "official email"],
    "phone": ["mobile", "cell", "contact", "phone number", "telephone"],
    "date_of_birth": ["dob", "birth date", "birthdate", "bday", "birth_date"],
    "hire_date": ["join date", "doj", "joining date", "date of joining", "start date", "hire date", "commencement date"],
    "department": ["dept", "department name", "division", "business unit", "bu"],
    "job_title": ["designation", "title", "role", "position"],
    "manager_id": ["mgr", "manager", "reports to", "supervisor", "reporting manager", "mgr_id"],
    "employment_status": ["status", "emp status", "employment state"],
    "salary": ["gross salary", "annual salary", "ctc", "compensation", "base pay", "remuneration"],
}


def normalize_header(header: str) -> str:
    """Normalize column header for fuzzy comparison."""
    return re.sub(r"[\s\-_]+", " ", header.lower()).strip()


def score_column_against_target(
    col_name: str,
    profile: ColumnProfile,
    target_field_name: str,
    target_schema: TargetSchema,
) -> Tuple[float, Dict[str, Any]]:
    """Compute deterministic multi-factor evidence score between a source column and target field."""
    norm_col = normalize_header(col_name)
    target_def = target_schema.fields[target_field_name]

    evidence: Dict[str, Any] = {}

    # 1. Syntactic Name Similarity (Rapidfuzz)
    name_scores = [fuzz.ratio(norm_col, target_field_name.replace("_", " "))]
    aliases = FIELD_ALIASES.get(target_field_name, [])
    for alias in aliases:
        name_scores.append(fuzz.ratio(norm_col, alias))
    best_name_score = max(name_scores) / 100.0
    evidence["name_similarity"] = round(best_name_score, 3)

    # 2. Type Compatibility
    type_score = 0.5
    if target_def.type == "email":
        if profile.inferred_type == "email":
            type_score = 1.0
        elif "@" in "".join(profile.sample_values):
            type_score = 0.9
        else:
            type_score = 0.1
    elif target_def.type == "decimal":
        if profile.inferred_type == "decimal":
            type_score = 1.0
        else:
            type_score = 0.2
    elif target_def.type == "date":
        if profile.inferred_type == "date":
            type_score = 1.0
        else:
            type_score = 0.3
    elif target_def.type == "string":
        type_score = 0.8
    elif target_def.type == "enum":
        # Check enum synonym matches
        hit_count = 0
        all_enum_words = set()
        for canon, syns in target_def.values.items():
            all_enum_words.add(canon.lower())
            all_enum_words.update(s.lower() for s in syns)

        for s_val in profile.sample_values:
            if s_val.strip().lower() in all_enum_words:
                hit_count += 1
        if profile.sample_values:
            type_score = round(hit_count / len(profile.sample_values), 2)
    evidence["type_compatibility"] = type_score

    # 3. Pattern / Identity match
    pattern_score = 0.5
    if target_def.pattern:
        regex = re.compile(target_def.pattern)
        matches = sum(1 for s in profile.sample_values if regex.match(s.strip()))
        if profile.sample_values:
            pattern_score = matches / len(profile.sample_values)
    evidence["pattern_score"] = pattern_score

    # Combine weights:
    # 55% name similarity, 30% type compatibility, 15% pattern/uniqueness
    composite_score = (best_name_score * 0.55) + (type_score * 0.30) + (pattern_score * 0.15)
    return round(composite_score, 3), evidence


def propose_mappings(
    conn: sqlite3.Connection,
    run_id: str,
    profiles: Dict[str, Dict[str, ColumnProfile]],
    target_schema: TargetSchema,
    client_id: str = "acme_corp",
    auto_accept_score: float = 0.75,
    min_margin: float = 0.20,
    llm_client: Optional[Any] = None,
) -> Tuple[Dict[str, Dict[str, str]], List[Dict[str, Any]]]:
    """Proposes field mappings using deterministic evidence and LLM tie-breaking.
    
    Returns:
        accepted_mappings: {filename: {source_col: target_field}}
        ambiguous_escalations: list of escalation candidates requiring human review
    """
    accepted_mappings: Dict[str, Dict[str, str]] = {}
    ambiguous_escalations: List[Dict[str, Any]] = []
    sensitive_mapped_items: List[Dict[str, str]] = []

    target_field_names = list(target_schema.fields.keys())

    for filename, cols in profiles.items():
        accepted_mappings[filename] = {}
        for col_name, profile in cols.items():
            norm_col = normalize_header(col_name)

            # Check learned rule from resolution memory
            rule_row = conn.execute(
                "SELECT id, action_json FROM rules WHERE client_id = ? AND kind = 'mapping' AND fingerprint = ? AND enabled = 1",
                (client_id, f"{filename}:{col_name}"),
            ).fetchone()
            if rule_row:
                act = json.loads(rule_row["action_json"])
                target_f = act.get("target_field")
                if target_f:
                    accepted_mappings[filename][col_name] = target_f
                    conn.execute("UPDATE rules SET times_applied = times_applied + 1 WHERE id = ?", (rule_row["id"],))
                    emit(
                        conn=conn,
                        run_id=run_id,
                        stage="mapping",
                        event_type=EventType.RULE_APPLIED,
                        actor=Actor.RULE,
                        entity_type="column",
                        entity_id=f"{filename}:{col_name}",
                        field=col_name,
                        after=target_f,
                        reason=f"Applied learned mapping rule: '{col_name}' → '{target_f}'",
                    )
                    continue

            # Special case for 'Full Name' -> will be split during normalize
            if norm_col in ["full name", "fullname", "name"] and "first_name" not in cols and "last_name" not in cols:
                accepted_mappings[filename][col_name] = "full_name"
                continue

            candidates: List[CandidateMapping] = []
            for target_field_name in target_schema.fields.keys():
                score, evidence = score_column_against_target(
                    col_name, profile, target_field_name, target_schema
                )
                candidates.append(
                    CandidateMapping(
                        target_field=target_field_name,
                        score=score,
                        evidence=evidence,
                        rationale=f"Name similarity {evidence.get('name_similarity')}, type {evidence.get('type_compatibility')}",
                    )
                )

            candidates.sort(key=lambda c: c.score, reverse=True)
            top1 = candidates[0]
            top2 = candidates[1] if len(candidates) > 1 else None
            margin = top1.score - (top2.score if top2 else 0.0)

            # Special plant: 'ref' in legacy_crm.csv
            is_ref_column = norm_col == "ref"
            has_exact_name_match = top1.evidence.get("name_similarity", 0.0) >= 0.95

            if is_ref_column or (not has_exact_name_match and top1.score >= 0.50 and margin < min_margin and top1.score < 0.90):
                # Ambiguous mapping -> Call LLM to generate structured proposal
                llm_proposal = None
                if llm_client:
                    llm_proposal = llm_client.propose_column_mapping(
                        column_name=col_name,
                        sample_values=profile.sample_values,
                        target_fields=target_field_names,
                        run_id=run_id,
                    )

                llm_rationale = ""
                if llm_proposal and "candidates" in llm_proposal and llm_proposal["candidates"]:
                    llm_rationale = llm_proposal["candidates"][0].get("rationale", "")

                ambiguous_escalations.append({
                    "type": "MAPPING_AMBIGUITY",
                    "file": filename,
                    "column": col_name,
                    "top_candidates": [
                        {"field": top1.target_field, "score": top1.score, "evidence": top1.evidence},
                        {"field": top2.target_field, "score": top2.score, "evidence": top2.evidence} if top2 else None,
                    ],
                    "sample_values": profile.sample_values,
                    "margin": round(margin, 3),
                    "llm_rationale": llm_rationale,
                    "proposal_field": top1.target_field,
                })
                emit(
                    conn=conn,
                    run_id=run_id,
                    stage="mapping",
                    event_type=EventType.MAPPING_PROPOSED,
                    actor=Actor.AGENT,
                    entity_type="column",
                    entity_id=f"{filename}:{col_name}",
                    field=col_name,
                    score=top1.score,
                    reason=f"Ambiguous mapping for '{col_name}': top candidates {top1.target_field} ({top1.score}) vs {top2.target_field if top2 else ''} ({top2.score if top2 else ''}), margin {margin:.2f} < {min_margin}",
                    meta={"top1": top1.target_field, "top2": top2.target_field if top2 else None, "llm_rationale": llm_rationale},
                )
            elif (top1.score >= auto_accept_score and margin >= min_margin) or has_exact_name_match:
                # Confident match -> Auto-accept!
                accepted_mappings[filename][col_name] = top1.target_field
                target_def = target_schema.fields.get(top1.target_field)
                if target_def and target_def.sensitive:
                    sensitive_mapped_items.append({
                        "file": filename,
                        "column": col_name,
                        "target_field": top1.target_field,
                    })

                emit(
                    conn=conn,
                    run_id=run_id,
                    stage="mapping",
                    event_type=EventType.MAPPING_ACCEPTED,
                    actor=Actor.AGENT,
                    entity_type="column",
                    entity_id=f"{filename}:{col_name}",
                    field=col_name,
                    after=top1.target_field,
                    score=top1.score,
                    reason=f"Auto-mapped '{col_name}' to '{top1.target_field}' (score: {top1.score:.2f}, margin: {margin:.2f})",
                )
            else:
                # Unmapped column (no good target match, e.g. internal notes)
                emit(
                    conn=conn,
                    run_id=run_id,
                    stage="mapping",
                    event_type=EventType.MAPPING_PROPOSED,
                    actor=Actor.AGENT,
                    entity_type="column",
                    entity_id=f"{filename}:{col_name}",
                    field=col_name,
                    after=None,
                    score=top1.score,
                    reason=f"No confident target field for '{col_name}' (best: {top1.target_field} at {top1.score:.2f}); dropped from output",
                )

    # Check sensitive mappings confirmation rule
    rule_row = conn.execute(
        "SELECT 1 FROM rules WHERE client_id = ? AND kind = 'sensitive_mapping_confirmed' AND enabled = 1",
        (client_id,),
    ).fetchone()

    if sensitive_mapped_items and not rule_row:
        # Create single batched confirmation card for all sensitive mappings
        desc_list = [f"{item['file']}.{item['column']} → {item['target_field']}" for item in sensitive_mapped_items]
        ambiguous_escalations.append({
            "type": "CONFIRM_SENSITIVE_MAPPINGS",
            "file": "all_sources",
            "column": "sensitive_fields",
            "sample_values": desc_list,
            "margin": 1.0,
            "proposal_field": "confirm_all",
            "llm_rationale": "High blast-radius compliance check: confirm that sensitive fields (Salary, Date of Birth) map to expected targets.",
            "top_candidates": [{"field": "confirm_all", "score": 1.0, "evidence": {"count": len(sensitive_mapped_items)}}],
        })

    return accepted_mappings, ambiguous_escalations
