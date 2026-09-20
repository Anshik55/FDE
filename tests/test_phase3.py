"""Phase 3 verification tests: multi-file ingestion, normalization, reconciliation, and validation."""

from pathlib import Path
import sqlite3
import pytest

from app.db import init_db
from app.pipeline.orchestrator import run_pipeline

BASE_DIR = Path(__file__).resolve().parent.parent


def test_end_to_end_reconciliation_and_planted_cases(tmp_path):
    test_db = tmp_path / "test_phase3.db"
    conn = init_db(test_db)

    # Run the full pipeline without pushing to external target
    result = run_pipeline(conn=conn, push_to_target=False, client_id="acme_corp")

    assert result["status"] == "completed"
    assert result["total_source_rows"] == 82  # 35 HRIS + 25 Payroll + 22 CRM
    assert result["canonical_records"] > 0
    assert result["escalations_opened"] >= 5

    # 1. Verify Planted Case 8: E1020 missing email in HRIS was filled from CRM
    can_e1020 = conn.execute(
        "SELECT data_json, provenance_json FROM canonical_records WHERE canonical_key = 'E1020' AND run_id = ?",
        (result["run_id"],),
    ).fetchone()
    assert can_e1020 is not None
    data_e1020 = eval(can_e1020["data_json"])
    assert data_e1020.get("email") == "vikram.mehta@example.com", "Missing email in HRIS should be filled from CRM"

    # 2. Verify Planted Case 5: Sensitive Salary Conflict for E1005
    sal_conf = conn.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND type = 'SENSITIVE_FIELD_CONFLICT' AND group_key LIKE '%E1005%'",
        (result["run_id"],),
    ).fetchone()
    assert sal_conf is not None, "Salary conflict for E1005 must open an escalation"

    # 3. Verify Planted Case 4: Near-duplicate Rahul Sharma vs Rahul Sherma
    near_dup = conn.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND type = 'NEAR_DUPLICATE_RECORD'",
        (result["run_id"],),
    ).fetchone()
    assert near_dup is not None, "Near-duplicate record must open an escalation"

    # 4. Verify Planted Case 2: Ambiguous Date Format in payroll_export.xlsx (DOJ)
    date_esc = conn.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND type = 'DATE_FORMAT_AMBIGUOUS'",
        (result["run_id"],),
    ).fetchone()
    assert date_esc is not None, "Payroll DOJ (all days <= 12) must open a date format escalation"

    # 5. Verify Planted Case 6: Status "LOA" grouped escalation with LLM proposal
    loa_esc = conn.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND type = 'ENUM_VALUE_AMBIGUOUS' AND group_key LIKE '%LOA%'",
        (result["run_id"],),
    ).fetchone()
    assert loa_esc is not None, "LOA status must open an enum escalation"
    assert "On Leave" in loa_esc["proposal_json"]

    # 6. Verify Planted Case 7: E1030 missing email everywhere fails validation twice
    val_fail = conn.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND type = 'VALIDATION_FAILED_TWICE' AND group_key LIKE '%E1030%'",
        (result["run_id"],),
    ).fetchone()
    assert val_fail is not None, "E1030 missing required email must fail validation twice and escalate"
