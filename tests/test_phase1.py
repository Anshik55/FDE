"""Phase 1 verification tests: thin end-to-end slice, append-only events, and stub integration."""

import sqlite3
from pathlib import Path
from fastapi.testclient import TestClient
import pytest

from app.db import init_db
from app.events import Actor, EventType, emit, tail_events
from app.main import app
from app.pipeline.escalation import resolve_escalation
from app.pipeline.orchestrator import run_pipeline

BASE_DIR = Path(__file__).resolve().parent.parent


def test_append_only_event_log_triggers(tmp_path):
    test_db = tmp_path / "test_events.db"
    conn = init_db(test_db)

    # Insert test run
    conn.execute("INSERT INTO runs (id, client_id, status, started_at) VALUES ('r1', 'test_c', 'running', 'now')")
    conn.commit()

    ev_id = emit(
        conn=conn,
        run_id="r1",
        stage="test",
        event_type=EventType.RUN_STARTED,
        actor=Actor.AGENT,
        reason="Test event",
    )
    assert ev_id > 0

    # Attempt UPDATE on events -> MUST FAIL with SQLite trigger error
    with pytest.raises(sqlite3.DatabaseError) as exc_info:
        conn.execute("UPDATE events SET reason = 'tampered' WHERE id = ?", (ev_id,))
    assert "append-only" in str(exc_info.value).lower()

    # Attempt DELETE on events -> MUST FAIL with SQLite trigger error
    with pytest.raises(sqlite3.DatabaseError) as exc_info:
        conn.execute("DELETE FROM events WHERE id = ?", (ev_id,))
    assert "append-only" in str(exc_info.value).lower()


def test_pipeline_headless_run_and_stub_push(tmp_path):
    test_db = tmp_path / "test_migration.db"
    conn = init_db(test_db)

    client = TestClient(app)
    # Clear stub data first
    client.delete("/stub/admin/clear")

    # Run the pipeline
    result = run_pipeline(conn=conn, push_to_target=False)

    assert result["status"] == "completed"
    assert result["records_processed"] > 0
    assert result["escalations_opened"] >= 1

    # Verify source rows were stored
    count_rows = conn.execute("SELECT COUNT(*) as cnt FROM source_rows WHERE run_id = ?", (result["run_id"],)).fetchone()["cnt"]
    assert count_rows == (35 + 25 + 22)  # 82 total source rows

    # Verify ambiguous mapping escalation for 'ref' was opened
    esc = conn.execute("SELECT * FROM escalations WHERE run_id = ? AND group_key LIKE '%ref%'", (result["run_id"],)).fetchone()
    assert esc is not None
    assert esc["status"] == "pending"

    # Resolve escalation
    res = resolve_escalation(
        conn=conn,
        escalation_id=esc["id"],
        action="correct",
        value="employee_id",
        remember=True,
    )
    assert res["status"] == "resolved"

    # Verify HUMAN_RESOLVED event was emitted
    resolved_ev = conn.execute(
        "SELECT * FROM events WHERE run_id = ? AND type = 'HUMAN_RESOLVED'", (result["run_id"],)
    ).fetchone()
    assert resolved_ev is not None
    assert resolved_ev["actor"] == "human"


def test_stub_api_happy_path_and_idempotency():
    client = TestClient(app)
    client.delete("/stub/admin/clear")

    emp_payload = {
        "employee_id": "E1001",
        "first_name": "Aarav",
        "last_name": "Sharma",
        "email": "aarav.sharma@acmecorp.com",
    }
    idem_key = "test_key_123"

    # 1. Create employee
    resp1 = client.post("/stub/employees", json=emp_payload, headers={"Idempotency-Key": idem_key})
    assert resp1.status_code == 201
    assert resp1.json()["status"] == "created"

    # 2. Resend identical request with same idempotency key -> returns 200 without duplicate insertion
    resp2 = client.post("/stub/employees", json=emp_payload, headers={"Idempotency-Key": idem_key})
    assert resp2.status_code == 200
    assert resp2.json()["status"] == "already_processed"

    # Verify count is 1
    list_resp = client.get("/stub/employees")
    assert len(list_resp.json()) == 1
