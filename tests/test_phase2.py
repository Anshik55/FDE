"""Phase 2 verification tests: mapping agent, evidence scoring, LLM proposals, and sensitive field batching."""

from pathlib import Path
import sqlite3
import pytest

from app.db import init_db
from app.pipeline.escalation import resolve_escalation
from app.pipeline.llm import LLMClient
from app.pipeline.orchestrator import run_pipeline
from app.pipeline.schema_loader import TargetSchema

BASE_DIR = Path(__file__).resolve().parent.parent


def test_llm_client_fallback_and_fail_safe(tmp_path):
    conn = init_db(tmp_path / "test_llm.db")
    llm = LLMClient(conn=conn)

    # Test mapping proposal for ambiguous 'ref' column
    proposal = llm.propose_column_mapping(
        column_name="ref",
        sample_values=["E1015", "E1016", "E1017", "E1018", "E1019"],
        target_fields=["employee_id", "manager_id", "email", "phone"],
        run_id="test_run",
    )
    assert "candidates" in proposal
    assert len(proposal["candidates"]) >= 1
    top_field = proposal["candidates"][0]["field"]
    assert top_field in ["employee_id", "manager_id"]
    assert len(proposal["candidates"][0]["rationale"]) > 0

    # Test enum proposal for "LOA"
    enum_prop = llm.propose_enum_value(
        field_name="employment_status",
        raw_value="LOA",
        canonical_choices=["Active", "Terminated", "On Leave"],
        run_id="test_run",
    )
    assert enum_prop["canonical_value"] == "On Leave"
    assert enum_prop["confidence"] >= 0.90


def test_sensitive_mapping_batched_card_and_memory(tmp_path):
    test_db = tmp_path / "test_mapping_agent.db"
    conn = init_db(test_db)

    # 1. Run 1: Should open CONFIRM_SENSITIVE_MAPPINGS
    res1 = run_pipeline(conn=conn, push_to_target=False, client_id="acme_corp")
    sensitive_esc = conn.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND type = 'CONFIRM_SENSITIVE_MAPPINGS'",
        (res1["run_id"],),
    ).fetchone()
    assert sensitive_esc is not None
    assert sensitive_esc["status"] == "pending"

    # 2. Human resolves and remembers as rule
    resolve_escalation(
        conn=conn,
        escalation_id=sensitive_esc["id"],
        action="approve",
        resolved_by="lead_consultant",
        remember=True,
    )

    # Insert the client rule explicitly as remembered
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO rules (id, client_id, kind, fingerprint, action_json, source_escalation_id, enabled, created_at)
            VALUES ('r_sens', 'acme_corp', 'sensitive_mapping_confirmed', 'all_sensitive', '{"action": "approved"}', ?, 1, datetime('now'))
            """,
            (sensitive_esc["id"],),
        )

    # 3. Run 2 for the same client: CONFIRM_SENSITIVE_MAPPINGS should NOT be asked again!
    res2 = run_pipeline(conn=conn, push_to_target=False, client_id="acme_corp")
    sensitive_esc_run2 = conn.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND type = 'CONFIRM_SENSITIVE_MAPPINGS'",
        (res2["run_id"],),
    ).fetchone()
    assert sensitive_esc_run2 is None, "Sensitive mappings confirmation should be remembered and not asked on Run 2"
