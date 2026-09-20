"""Phase 5 Tests: Push Semantics, 503 retry, 422 escalation, idempotency, and compensating rollback."""

import json
import sqlite3
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.db import init_db
from app.events import EventType
from app.pipeline.push import (
    push_records,
    rollback_run,
    retry_failed_records,
    compute_idempotency_key,
)


@pytest.fixture
def test_db(tmp_path, monkeypatch):
    db_path = tmp_path / "test_phase5.db"
    monkeypatch.setattr("app.db.DB_PATH", db_path)
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


@pytest.fixture
def client(test_db):
    return TestClient(app)


def _setup_test_run(test_db, run_id="run_p5_test"):
    with test_db:
        test_db.execute(
            "INSERT OR REPLACE INTO runs (id, client_id, status, started_at) VALUES (?, 'test_client', 'running', datetime('now'))",
            (run_id,),
        )
    return run_id


def test_push_503_retry_silent_success(test_db, client):
    """Planted Case 10 part A: 503 transient failure is silently retried with backoff and succeeds."""
    run_id = _setup_test_run(test_db, "run_503")
    emp = {
        "employee_id": "E5001",
        "first_name": "Kavita",
        "last_name": "Rao",
        "email": "kavita.rao@example.com",
        "hire_date": "2023-01-15",
        "department": "Engineering",
        "employment_status": "Active",
    }
    # Configure stub to fail E5001 with 503 on first attempt
    client.post("/stub/admin/failure-config", json={"fail_503_ids": ["E5001"]})

    # Prepare canonical record
    with test_db:
        test_db.execute(
            """
            INSERT INTO canonical_records (id, run_id, canonical_key, data_json, provenance_json, status)
            VALUES (?, ?, ?, ?, '{}', 'ready')
            """,
            ("rec_5001", run_id, "E5001", json.dumps(emp)),
        )

    result = push_records(
        conn=test_db,
        run_id=run_id,
        records=[{"canonical_key": "E5001", "data": emp}],
        client=client,
        max_retries=3,
        backoff_base_s=0.01,
    )

    assert result["pushed_count"] == 1
    assert "E5001" in result["pushed_ids"]

    # Verify push_attempts log shows attempt 1 failed (503) and attempt 2 succeeded (201)
    attempts = test_db.execute(
        "SELECT attempt, http_status FROM push_attempts WHERE run_id = ? AND record_id = 'E5001' ORDER BY attempt ASC",
        (run_id,),
    ).fetchall()
    assert len(attempts) == 2
    assert attempts[0]["http_status"] == 503
    assert attempts[1]["http_status"] == 201

    # Verify canonical record marked pushed
    row = test_db.execute("SELECT status FROM canonical_records WHERE canonical_key = 'E5001'").fetchone()
    assert row["status"] == "pushed"


def test_push_422_no_retry_and_escalates(test_db, client):
    """Planted Case 10 part B: 422 client error is never retried, record is marked rejected and escalated."""
    run_id = _setup_test_run(test_db, "run_422")
    emp = {
        "employee_id": "E4220",
        "first_name": "Aman",
        "last_name": "Gupta",
        "email": "aman.gupta@example.com",
        "hire_date": "2023-01-15",
        "department": "InvalidDept",
        "employment_status": "Active",
    }
    client.post("/stub/admin/failure-config", json={"fail_422_ids": ["E4220"]})

    with test_db:
        test_db.execute(
            """
            INSERT INTO canonical_records (id, run_id, canonical_key, data_json, provenance_json, status)
            VALUES (?, ?, ?, ?, '{}', 'ready')
            """,
            ("rec_4220", run_id, "E4220", json.dumps(emp)),
        )

    result = push_records(
        conn=test_db,
        run_id=run_id,
        records=[{"canonical_key": "E4220", "data": emp}],
        client=client,
        max_retries=3,
        backoff_base_s=0.01,
    )

    assert result["failed_count"] == 1
    assert "E4220" in result["failed_ids"]

    # Exactly 1 attempt made (NO retries for 4xx)
    attempts = test_db.execute(
        "SELECT attempt, http_status FROM push_attempts WHERE run_id = ? AND record_id = 'E4220'",
        (run_id,),
    ).fetchall()
    assert len(attempts) == 1
    assert attempts[0]["http_status"] == 422

    # Status marked rejected
    row = test_db.execute("SELECT status FROM canonical_records WHERE canonical_key = 'E4220'").fetchone()
    assert row["status"] == "rejected"

    # Escalation opened
    esc = test_db.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND type = 'PUSH_REJECTED_4XX'",
        (run_id,),
    ).fetchone()
    assert esc is not None
    assert "E4220" in esc["affected_ids_json"]


def test_push_idempotent_replay(test_db, client):
    """Pushing same record twice with same payload returns 200 without creating duplicates."""
    run_id = _setup_test_run(test_db, "run_idem")
    client.post("/stub/admin/clear")

    emp = {
        "employee_id": "E7701",
        "first_name": "Rohan",
        "last_name": "Deshmukh",
        "email": "rohan.d@example.com",
        "hire_date": "2022-04-01",
        "department": "Finance",
        "employment_status": "Active",
    }
    with test_db:
        test_db.execute(
            "INSERT INTO canonical_records (id, run_id, canonical_key, data_json, provenance_json, status) VALUES ('r1', ?, 'E7701', ?, '{}', 'ready')",
            (run_id, json.dumps(emp)),
        )

    # First push
    res1 = push_records(conn=test_db, run_id=run_id, records=[{"canonical_key": "E7701", "data": emp}], client=client)
    assert res1["pushed_count"] == 1

    # Second push (identical payload)
    res2 = push_records(conn=test_db, run_id=run_id, records=[{"canonical_key": "E7701", "data": emp}], client=client)
    assert res2["pushed_count"] == 1

    # Only one record in target system
    target_records = client.get("/stub/employees").json()
    matching = [r for r in target_records if r["employee_id"] == "E7701"]
    assert len(matching) == 1


def test_push_outage_circuit_breaker_and_compensating_rollback(test_db, client):
    """Planted Case 11: Outage after N records triggers compensating rollback of current batch."""
    run_id = _setup_test_run(test_db, "run_outage")
    client.post("/stub/admin/clear")

    # Configure outage after 1 record is created
    client.post("/stub/admin/failure-config", json={"outage_after_count": 1})

    batch_emps = [
        {"employee_id": f"E800{i}", "first_name": f"User{i}", "last_name": "Test", "email": f"u{i}@example.com", "hire_date": "2023-01-01", "department": "HR", "employment_status": "Active"}
        for i in range(1, 5)
    ]
    for emp in batch_emps:
        with test_db:
            test_db.execute(
                "INSERT INTO canonical_records (id, run_id, canonical_key, data_json, provenance_json, status) VALUES (?, ?, ?, ?, '{}', 'ready')",
                (f"r_{emp['employee_id']}", run_id, emp["employee_id"], json.dumps(emp)),
            )

    result = push_records(
        conn=test_db,
        run_id=run_id,
        records=[{"canonical_key": e["employee_id"], "data": e} for e in batch_emps],
        client=client,
        batch_size=10,
        max_retries=2,
        backoff_base_s=0.01,
        outage_consecutive_failures=2,
    )

    # E8001 was initially created, but then subsequent repeated failures rolled it back
    target_after = client.get("/stub/employees").json()
    assert len(target_after) == 0  # Clean rollback!

    # Verify BATCH_ROLLED_BACK event emitted
    ev = test_db.execute(
        "SELECT * FROM events WHERE run_id = ? AND type = ?",
        (run_id, EventType.BATCH_ROLLED_BACK),
    ).fetchone()
    assert ev is not None

    # Verify TARGET_OUTAGE_HALT escalation opened
    esc = test_db.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND type = 'TARGET_OUTAGE_HALT'",
        (run_id,),
    ).fetchone()
    assert esc is not None


def test_manual_run_rollback(test_db, client):
    """User can trigger full compensating rollback of an entire run."""
    run_id = _setup_test_run(test_db, "run_manual_rollback")
    client.post("/stub/admin/clear")

    emps = [
        {"employee_id": "E9001", "first_name": "First", "last_name": "User", "email": "f@example.com", "hire_date": "2023-01-01", "department": "Sales", "employment_status": "Active"},
        {"employee_id": "E9002", "first_name": "Second", "last_name": "User", "email": "s@example.com", "hire_date": "2023-01-01", "department": "Sales", "employment_status": "Active"},
    ]
    for emp in emps:
        with test_db:
            test_db.execute(
                "INSERT INTO canonical_records (id, run_id, canonical_key, data_json, provenance_json, status) VALUES (?, ?, ?, ?, '{}', 'ready')",
                (f"r_{emp['employee_id']}", run_id, emp["employee_id"], json.dumps(emp)),
            )

    push_records(conn=test_db, run_id=run_id, records=[{"canonical_key": e["employee_id"], "data": e} for e in emps], client=client)
    assert len(client.get("/stub/employees").json()) == 2

    # Now execute manual run rollback
    rollback_res = rollback_run(conn=test_db, run_id=run_id, client=client)
    assert rollback_res["deleted_count"] == 2

    # Stub should be empty
    assert len(client.get("/stub/employees").json()) == 0

    # Records in canonical_records updated to rolled_back
    statuses = [r["status"] for r in test_db.execute("SELECT status FROM canonical_records WHERE run_id = ?", (run_id,)).fetchall()]
    assert all(s == "rolled_back" for s in statuses)
