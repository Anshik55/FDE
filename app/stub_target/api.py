"""Stub target system (simulates external Darwinbox/HR API with failure injection)."""

from datetime import datetime, timezone
import json
import sqlite3
import uuid
from fastapi import APIRouter, Header, HTTPException, Request, Response, status
from pydantic import BaseModel
from typing import Any, Dict, List, Optional

from app.db import get_db_connection

router = APIRouter(prefix="/stub", tags=["Stub Target API"])


class FailureConfig(BaseModel):
    fail_503_ids: List[str] = []       # Employee IDs that should trigger a 503 once
    fail_422_ids: List[str] = []       # Employee IDs that should trigger a 422
    outage_after_count: Optional[int] = None  # Trigger 503 for all requests after N creates


@router.post("/admin/failure-config")
def set_failure_config(config: FailureConfig):
    conn = get_db_connection()
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO stub_failure_config (key, config_json) VALUES ('config', ?)",
            (json.dumps(config.model_dump()),),
        )
    return {"status": "ok", "config": config.model_dump()}


@router.api_route("/admin/clear", methods=["POST", "DELETE"])
def clear_stub_data():
    conn = get_db_connection()
    with conn:
        conn.execute("DELETE FROM target_records")
        conn.execute("DELETE FROM stub_failure_config")
    return {"status": "cleared"}


def get_current_failure_config(conn: sqlite3.Connection) -> Dict[str, Any]:
    row = conn.execute("SELECT config_json FROM stub_failure_config WHERE key = 'config'").fetchone()
    if row:
        return json.loads(row["config_json"])
    return {}


@router.post("/employees", status_code=status.HTTP_201_CREATED)
async def create_employee(
    request: Request,
    response: Response,
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
):
    body = await request.json()
    emp_id = body.get("employee_id")
    if not emp_id:
        raise HTTPException(status_code=400, detail="Missing required employee_id")

    conn = get_db_connection()

    # 1. Check Idempotency Key
    if idempotency_key:
        existing = conn.execute(
            "SELECT * FROM target_records WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        if existing:
            response.status_code = status.HTTP_200_OK
            return {
                "id": existing["id"],
                "employee_id": existing["employee_id"],
                "status": "already_processed",
                "record": json.loads(existing["payload_json"]),
            }

    # 2. Check Failure Injections
    cfg = get_current_failure_config(conn)

    # 422 injection
    if emp_id in cfg.get("fail_422_ids", []):
        raise HTTPException(
            status_code=422,
            detail=f"Target API validation rejected employee {emp_id}: department or designation invalid",
        )

    # 503 injection (once per ID)
    if emp_id in cfg.get("fail_503_ids", []):
        # Remove from config so it succeeds on next retry
        new_503 = [x for x in cfg["fail_503_ids"] if x != emp_id]
        cfg["fail_503_ids"] = new_503
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO stub_failure_config (key, config_json) VALUES ('config', ?)",
                (json.dumps(cfg),),
            )
        raise HTTPException(
            status_code=503,
            detail=f"Temporary service unavailable for employee {emp_id}, retry allowed",
        )

    # Outage after N count
    outage_after = cfg.get("outage_after_count")
    if outage_after is not None:
        count_row = conn.execute("SELECT COUNT(*) as cnt FROM target_records").fetchone()
        if count_row["cnt"] >= outage_after:
            raise HTTPException(
                status_code=503,
                detail="System outage: target API is experiencing high load / downtime",
            )

    # 3. Insert record
    record_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).isoformat()
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO target_records (id, employee_id, payload_json, idempotency_key, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (record_id, emp_id, json.dumps(body), idempotency_key, ts),
            )
    except sqlite3.IntegrityError:
        # Unique constraint on employee_id failed
        raise HTTPException(status_code=409, detail=f"Employee {emp_id} already exists in target system")

    return {
        "id": record_id,
        "employee_id": emp_id,
        "status": "created",
        "created_at": ts,
        "record": body,
    }


@router.get("/employees")
def list_employees():
    conn = get_db_connection()
    rows = conn.execute("SELECT * FROM target_records ORDER BY created_at ASC").fetchall()
    return [
        {
            "id": r["id"],
            "employee_id": r["employee_id"],
            "idempotency_key": r["idempotency_key"],
            "created_at": r["created_at"],
            "record": json.loads(r["payload_json"]),
        }
        for r in rows
    ]


@router.delete("/employees/{emp_id}")
def delete_employee(emp_id: str):
    """Compensating delete for batch rollback."""
    conn = get_db_connection()
    with conn:
        cursor = conn.execute("DELETE FROM target_records WHERE employee_id = ? OR id = ?", (emp_id, emp_id))
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Record not found")
    return {"status": "deleted", "target_id": emp_id}
