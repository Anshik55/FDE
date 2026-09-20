"""Phase 6 Tests: UI routes, reconciliation report, autonomy scoreboard, audit export, and rules management."""

import json
import sqlite3
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.db import init_db
from app.events import Actor, EventType, emit
from app.pipeline.orchestrator import run_pipeline


@pytest.fixture
def test_db(tmp_path, monkeypatch):
    db_path = tmp_path / "test_phase6.db"
    monkeypatch.setattr("app.db.DB_PATH", db_path)
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


@pytest.fixture
def client(test_db):
    return TestClient(app)


def test_reconciliation_report_endpoint(test_db, client):
    """GET /report renders reconciliation report and autonomy scoreboard with all metrics."""
    # Seed a pipeline run
    res = run_pipeline(conn=test_db, client_id="acme_corp", push_to_target=False)
    run_id = res["run_id"]

    resp = client.get(f"/report?run_id={run_id}")
    assert resp.status_code == 200
    html = resp.text

    # Verify key sections are present
    assert "Reconciliation Report &amp; Autonomy Scoreboard" in html or "Reconciliation Report" in html
    assert "Criterion 6: Autonomy Scoreboard" in html
    assert "Source Precedence Spot-Check Ledger" in html
    assert "Canonical Dataset Manifest" in html
    assert "Autonomy Score" in html


def test_audit_log_view_and_filtering(test_db, client):
    """GET /audit renders event timeline with actor filtering."""
    res = run_pipeline(conn=test_db, client_id="acme_corp", push_to_target=False)
    run_id = res["run_id"]

    # View all
    resp_all = client.get(f"/audit?run_id={run_id}")
    assert resp_all.status_code == 200
    assert "Full Event Audit Trail" in resp_all.text

    # View filtered by agent
    resp_agent = client.get(f"/audit?run_id={run_id}&actor=agent")
    assert resp_agent.status_code == 200
    assert "AGENT" in resp_agent.text


def test_audit_export_csv_and_json(test_db, client):
    """GET /api/audit/export.csv and .json return download streams."""
    res = run_pipeline(conn=test_db, client_id="acme_corp", push_to_target=False)
    run_id = res["run_id"]

    # JSON export
    resp_json = client.get(f"/api/audit/export.json?run_id={run_id}")
    assert resp_json.status_code == 200
    assert "application/json" in resp_json.headers["content-type"]
    events_data = resp_json.json()
    assert isinstance(events_data, list)
    assert len(events_data) > 0
    assert "run_id" in events_data[0]

    # CSV export
    resp_csv = client.get(f"/api/audit/export.csv?run_id={run_id}")
    assert resp_csv.status_code == 200
    assert "text/csv" in resp_csv.headers["content-type"]
    csv_text = resp_csv.text
    assert "id,run_id,ts,stage,type,actor" in csv_text


def test_rules_view_and_toggle(test_db, client):
    """GET /rules displays learned rules and POST /api/rules/{id}/toggle switches state."""
    # Seed a learned rule
    with test_db:
        test_db.execute(
            """
            INSERT INTO rules (id, client_id, kind, fingerprint, action_json, enabled, created_at, times_applied)
            VALUES ('rule_test_1', 'acme_corp', 'enum_value', 'employment_status:loa', '{"canonical_value": "On Leave"}', 1, datetime('now'), 3)
            """
        )

    # 1. View rules page
    resp = client.get("/rules?client_id=acme_corp")
    assert resp.status_code == 200
    assert "Resolution Memory & Learned Rules" in resp.text
    assert "employment_status:loa" in resp.text
    assert "Active" in resp.text

    # 2. Toggle rule to disabled
    resp_toggle = client.post("/api/rules/rule_test_1/toggle")
    assert resp_toggle.status_code == 200
    assert "Disabled" in resp_toggle.text

    # Verify DB updated
    row = test_db.execute("SELECT enabled FROM rules WHERE id = 'rule_test_1'").fetchone()
    assert row["enabled"] == 0

    # 3. Toggle rule back to active
    resp_toggle_back = client.post("/api/rules/rule_test_1/toggle")
    assert resp_toggle_back.status_code == 200
    assert "Active" in resp_toggle_back.text

    row = test_db.execute("SELECT enabled FROM rules WHERE id = 'rule_test_1'").fetchone()
    assert row["enabled"] == 1


def test_reset_db_endpoint(client):
    """POST /api/db/reset clears database and returns fresh dashboard HTML."""
    resp = client.post("/api/db/reset")
    assert resp.status_code == 200
    assert "Database reset complete" in resp.text
    assert "No Active Run" in resp.text


def test_upload_and_run_endpoint(tmp_path, client):
    """POST /api/upload-and-run accepts uploaded files and executes the pipeline."""
    # Create sample csv
    csv_file = tmp_path / "test_upload.csv"
    csv_file.write_text("EmpID,First Name,Last Name,Email,DOJ,Department,Status\nE9901,Priya,Sharma,priya@example.com,2023-01-10,Engineering,Active\n")

    with open(csv_file, "rb") as f:
        resp = client.post(
            "/api/upload-and-run",
            files={"files": ("test_upload.csv", f, "text/csv")},
        )
    assert resp.status_code == 200
    assert "Successfully ingested and processed 1 uploaded file(s)" in resp.text


def test_pipeline_pause_and_resume_flow(client):
    """Uploading a file with ambiguities pauses the pipeline for human review and allows resuming."""
    from pathlib import Path
    import re

    pilot_csv = Path(__file__).resolve().parent.parent / "fixtures" / "pilot_10_employees.csv"
    with open(pilot_csv, "rb") as f:
        resp = client.post(
            "/api/upload-and-run",
            files={"files": ("pilot_10_employees.csv", f, "text/csv")},
        )
    assert resp.status_code == 200
    html = resp.text
    assert "PAUSED: Human Review Required" in html
    assert 'id="human-intervention-panel"' in html
    assert "Decisions Pending" in html

    # Extract run_id
    match = re.search(r"run_[a-f0-9]+", html)
    assert match is not None
    run_id = match.group(0)

    # First attempt to resume without resolving should be blocked
    blocked_resp = client.post(f"/api/pipeline/resume/{run_id}")
    assert "Cannot resume" in blocked_resp.text

    # Resolve all pending escalations
    esc_ids = re.findall(r"/api/escalations/([a-f0-9\-]+)/resolve", html)
    assert len(esc_ids) > 0
    for esc_id in set(esc_ids):
        res = client.post(f"/api/escalations/{esc_id}/resolve", data={"action": "approve", "remember": False})
        assert res.status_code == 200

    # Resume the pipeline after all items resolved
    resume_resp = client.post(f"/api/pipeline/resume/{run_id}")
    assert resume_resp.status_code == 200
    resume_html = resume_resp.text
    assert "Pipeline resumed" in resume_html
    assert "All 6 Stages Completed" in resume_html
    assert 'id="human-intervention-panel"' not in resume_html


def test_records_view_and_exports(client):
    """GET /records displays canonical dataset and /api/export/records exports data cleanly."""
    # 1. Check /records view
    resp = client.get("/records")
    assert resp.status_code == 200
    assert "Processed Target Records" in resp.text
    assert "Darwinbox Integration Active" in resp.text

    # 2. Check CSV export
    csv_resp = client.get("/api/export/records.csv")
    assert csv_resp.status_code == 200
    assert "text/csv" in csv_resp.headers["content-type"]
    assert "employee_id,first_name,last_name" in csv_resp.text

    # 3. Check JSON export
    json_resp = client.get("/api/export/records.json")
    assert json_resp.status_code == 200
    data = json_resp.json()
    assert "records" in data
    assert "run_id" in data


def test_push_rejection_pauses_pipeline_and_repush_on_resolve(client):
    """When a 4xx target API rejection occurs, pipeline must pause at Stage 6, allowing correction and re-push."""
    import json
    from app.db import get_db_connection
    from app.pipeline.orchestrator import resume_pipeline

    from datetime import datetime, timezone
    client.post("/api/db/reset")
    conn = get_db_connection()
    run_id = "run_push_test"
    now_iso = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.execute("INSERT INTO runs (id, client_id, status, started_at) VALUES (?, 'acme_corp', 'paused', ?)", (run_id, now_iso))
        emp1 = {
            "employee_id": "E101",
            "first_name": "Test",
            "last_name": "One",
            "email": "test.one@example.com",
            "hire_date": "2023-01-01",
            "employment_status": "Active",
            "department": "Engineering",
        }
        emp2 = {
            "employee_id": "E102",
            "first_name": "Test",
            "last_name": "Two",
            "email": "test.two@example.com",
            "hire_date": "2023-01-01",
            "employment_status": "Active",
            "department": "InvalidDept",
        }
        conn.execute(
            "INSERT INTO canonical_records (id, run_id, canonical_key, data_json, provenance_json, status) VALUES (?, ?, ?, ?, '{}', 'ready')",
            ("can_E101", run_id, "E101", json.dumps(emp1)),
        )
        conn.execute(
            "INSERT INTO canonical_records (id, run_id, canonical_key, data_json, provenance_json, status) VALUES (?, ?, ?, ?, '{}', 'ready')",
            ("can_E102", run_id, "E102", json.dumps(emp2)),
        )

    # Configure Darwinbox Stub to reject E102 with 422
    client.post("/stub/admin/failure-config", json={"fail_422_ids": ["E102"]})

    # Resume pipeline -> E101 succeeds, E102 fails with 422
    result = resume_pipeline(run_id=run_id, conn=conn, push_to_target=True, target_api_url="http://testserver/stub")
    assert result["status"] == "paused"
    assert result["escalations_opened"] == 1

    # Verify run remains paused in DB
    row = conn.execute("SELECT status FROM runs WHERE id = ?", (run_id,)).fetchone()
    assert row["status"] == "paused"

    # Find the PUSH_REJECTED_4XX escalation
    esc = conn.execute("SELECT * FROM escalations WHERE run_id = ? AND type = 'PUSH_REJECTED_4XX'", (run_id,)).fetchone()
    assert esc is not None
    esc_id = esc["id"]

    # Clear failure config so next push succeeds
    client.post("/stub/admin/failure-config", json={"fail_422_ids": []})

    # Resolve escalation with correction
    resolve_resp = client.post(
        f"/api/escalations/{esc_id}/resolve",
        data={"action": "correct", "correct_value": "Engineering", "remember": False},
    )
    assert resolve_resp.status_code == 200

    # Verify canonical record is reset to ready
    fresh_conn = get_db_connection()
    can_row = fresh_conn.execute("SELECT status FROM canonical_records WHERE run_id = ? AND canonical_key = 'E102'", (run_id,)).fetchone()
    assert can_row["status"] == "ready"

    # Now resume pipeline via endpoint
    resume_resp = client.post(f"/api/pipeline/resume/{run_id}")
    assert resume_resp.status_code == 200
    assert "All 6 Stages Completed" in resume_resp.text or "Pipeline resumed" in resume_resp.text

    # Verify run is now completed
    run_final = conn.execute("SELECT status FROM runs WHERE id = ?", (run_id,)).fetchone()
    assert run_final["status"] == "completed"



