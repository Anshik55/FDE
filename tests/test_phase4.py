"""Phase 4 verification tests: escalation engine, resolution feedback loop, and resolution memory."""

from pathlib import Path
import sqlite3
import pytest

from app.db import init_db
from app.events import EventType
from app.pipeline.escalation import resolve_escalation
from app.pipeline.orchestrator import run_pipeline

BASE_DIR = Path(__file__).resolve().parent.parent


def test_resolution_memory_shrinks_escalations_on_run_2(tmp_path):
    test_db = tmp_path / "test_phase4.db"
    conn = init_db(test_db)
    client_id = "acme_corp"

    # =========================================================================
    # Run 1: First migration without prior memory
    # =========================================================================
    res1 = run_pipeline(conn=conn, push_to_target=False, client_id=client_id)
    run1_id = res1["run_id"]

    run1_escalations = conn.execute(
        "SELECT id, type, group_key FROM escalations WHERE run_id = ? AND status = 'pending'",
        (run1_id,),
    ).fetchall()
    run1_card_count = len(run1_escalations)
    assert run1_card_count >= 6, f"Expected at least 6 initial escalation cards, got {run1_card_count}"

    # Verify LOA status was escalated
    loa_esc = conn.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND type = 'ENUM_VALUE_AMBIGUOUS' AND group_key LIKE '%LOA%'",
        (run1_id,),
    ).fetchone()
    assert loa_esc is not None

    # Verify 'ref' mapping was escalated
    ref_esc = conn.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND type = 'MAPPING_AMBIGUITY' AND group_key LIKE '%ref%'",
        (run1_id,),
    ).fetchone()
    assert ref_esc is not None

    # Verify sensitive confirmation card
    sens_esc = conn.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND type = 'CONFIRM_SENSITIVE_MAPPINGS'",
        (run1_id,),
    ).fetchone()
    assert sens_esc is not None

    # Verify DOJ date format card
    date_esc = conn.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND type = 'DATE_FORMAT_AMBIGUOUS'",
        (run1_id,),
    ).fetchone()
    assert date_esc is not None

    # =========================================================================
    # Human Resolves cards with remember=True
    # =========================================================================
    # 1. Resolve LOA -> "On Leave" with remember=True
    resolve_escalation(conn, loa_esc["id"], action="approve", value="On Leave", remember=True)

    # 2. Resolve 'ref' -> "employee_id" with remember=True
    resolve_escalation(conn, ref_esc["id"], action="correct", value="employee_id", remember=True)

    # 3. Resolve sensitive mappings -> approve with remember=True
    resolve_escalation(conn, sens_esc["id"], action="approve", remember=True)

    # 4. Resolve DOJ date -> approve with remember=True
    resolve_escalation(conn, date_esc["id"], action="approve", value="%d/%m/%Y", remember=True)

    # Check rules were synthesized
    rules = conn.execute("SELECT * FROM rules WHERE client_id = ?", (client_id,)).fetchall()
    assert len(rules) >= 4, f"Expected at least 4 synthesized rules, got {len(rules)}"

    # =========================================================================
    # Run 2: Second migration with active resolution memory
    # =========================================================================
    res2 = run_pipeline(conn=conn, push_to_target=False, client_id=client_id)
    run2_id = res2["run_id"]

    run2_escalations = conn.execute(
        "SELECT id, type, group_key FROM escalations WHERE run_id = ? AND status = 'pending'",
        (run2_id,),
    ).fetchall()
    run2_card_count = len(run2_escalations)

    # Assert that LOA produces ZERO escalations on Run 2
    loa_esc_run2 = conn.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND type = 'ENUM_VALUE_AMBIGUOUS' AND group_key LIKE '%LOA%'",
        (run2_id,),
    ).fetchone()
    assert loa_esc_run2 is None, "LOA must be automatically resolved by rule on Run 2"

    # Assert 'ref' produces ZERO escalations on Run 2
    ref_esc_run2 = conn.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND type = 'MAPPING_AMBIGUITY' AND group_key LIKE '%ref%'",
        (run2_id,),
    ).fetchone()
    assert ref_esc_run2 is None, "ref column must be automatically mapped by rule on Run 2"

    # Assert total cards dropped substantially (from ~7 to ~3, only record-level conflicts remain)
    assert run2_card_count < run1_card_count, f"Run 2 cards ({run2_card_count}) should be fewer than Run 1 ({run1_card_count})"
    assert run2_card_count <= 4, f"Expected at most 4 record-level cards on Run 2, got {run2_card_count}"

    # Verify RULE_APPLIED events were logged on Run 2
    rule_events = conn.execute(
        "SELECT COUNT(*) as cnt FROM events WHERE run_id = ? AND type = 'RULE_APPLIED'",
        (run2_id,),
    ).fetchone()["cnt"]
    assert rule_events >= 3, "Expected multiple RULE_APPLIED events during Run 2"
