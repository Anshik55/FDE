"""Main FastAPI application for AI Agent Client Data Migration."""

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import List, Optional
from fastapi import FastAPI, Form, File, UploadFile, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse, Response, JSONResponse
from fastapi.templating import Jinja2Templates

from app.db import get_db_connection, init_db, reset_database
from app.events import tail_events
from app.pipeline.escalation import resolve_escalation
from app.pipeline.orchestrator import run_pipeline, resume_pipeline
from app.pipeline.report import generate_reconciliation_report
from app.pipeline.push import rollback_run, retry_failed_records
from app.stub_target.api import router as stub_router

BASE_DIR = Path(__file__).resolve().parent.parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(title="AI Agent for Client Data Migration & Integration", lifespan=lifespan)
app.include_router(stub_router)
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))


def get_latest_run_id(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute("SELECT id FROM runs ORDER BY started_at DESC LIMIT 1").fetchone()
    return row["id"] if row else None


def get_dashboard_stats(conn: sqlite3.Connection, run_id: Optional[str]):
    if not run_id:
        return {
            "total_source_rows": 0,
            "autonomy_score": 100,
            "pending_escalations": 0,
            "pushed_records": 0,
        }

    total_rows = conn.execute(
        "SELECT COUNT(*) as cnt FROM source_rows WHERE run_id = ?", (run_id,)
    ).fetchone()["cnt"]

    pending_esc = conn.execute(
        "SELECT COUNT(*) as cnt FROM escalations WHERE run_id = ? AND status = 'pending'", (run_id,)
    ).fetchone()["cnt"]

    pushed = conn.execute(
        "SELECT COUNT(DISTINCT target_id) as cnt FROM push_attempts WHERE run_id = ? AND http_status IN (200, 201)",
        (run_id,),
    ).fetchone()["cnt"]

    # Autonomy score = % of actions resolved autonomously
    human_events = conn.execute(
        "SELECT COUNT(*) as cnt FROM events WHERE run_id = ? AND actor = 'human'", (run_id,)
    ).fetchone()["cnt"]

    agent_events = conn.execute(
        "SELECT COUNT(*) as cnt FROM events WHERE run_id = ? AND actor IN ('agent', 'rule')", (run_id,)
    ).fetchone()["cnt"]

    total_decisions = human_events + agent_events
    autonomy = round((agent_events / total_decisions) * 100, 1) if total_decisions > 0 else 100.0

    return {
        "total_source_rows": total_rows,
        "autonomy_score": autonomy,
        "pending_escalations": pending_esc,
        "pushed_records": pushed,
    }


def get_pending_escalations(conn: sqlite3.Connection, run_id: str) -> list:
    """Fetch all open pending escalations for a run."""
    if not run_id or run_id == "No Active Run":
        return []
    rows = conn.execute(
        "SELECT * FROM escalations WHERE run_id = ? AND status = 'pending' ORDER BY created_at DESC",
        (run_id,),
    ).fetchall()
    items = []
    for row in rows:
        context = json.loads(row["context_json"])
        proposal = json.loads(row["proposal_json"]) if row["proposal_json"] else None
        affected = json.loads(row["affected_ids_json"] or "[]")
        items.append({
            "id": row["id"],
            "type": row["type"],
            "group_key": row["group_key"],
            "status": row["status"],
            "context": context,
            "proposal": proposal,
            "affected_count": len(affected),
            "created_at": row["created_at"],
        })
    return items


def render_stages_html(
    run_id: str,
    is_running: bool = False,
    active_step: int = 6,
    is_paused: bool = False,
    pending_count: int = 0,
) -> str:
    """Renders user-friendly, non-technical pipeline stage progress cards."""
    has_run = bool(run_id and run_id != "No Active Run")

    stages_meta = [
        {
            "num": 1,
            "title": "1. Ingest Data",
            "subtitle": "Read source files",
            "desc": "Loads Excel (.xlsx) & CSV files into memory",
            "icon": "M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12",
        },
        {
            "num": 2,
            "title": "2. Profile Columns",
            "subtitle": "Inspect structure",
            "desc": "Scans column headers, formats & blank cells",
            "icon": "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z",
        },
        {
            "num": 3,
            "title": "3. AI Schema Mapping",
            "subtitle": "Match to Darwinbox",
            "desc": "Uses AI to match client columns to employee fields",
            "icon": "M13 10V3L4 14h7v7l9-11h-7z",
        },
        {
            "num": 4,
            "title": "4. Clean & Merge",
            "subtitle": "Standardize records",
            "desc": "Standardizes dates, titles & merges duplicate rows",
            "icon": "M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z",
        },
        {
            "num": 5,
            "title": "5. Quality Check",
            "subtitle": "Validate rules",
            "desc": "Checks required fields, auto-fixes phone/email errors",
            "icon": "M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z",
        },
        {
            "num": 6,
            "title": "6. Target API Push",
            "subtitle": "Save to Darwinbox",
            "desc": "Creates verified employee records in Darwinbox API",
            "icon": "M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12",
        },
    ]

    if not has_run:
        progress_pct = 0
        progress_label = "0% (Ready)"
        status_badge = '<span id="pipeline-status-badge" class="px-2.5 py-0.5 rounded-full text-[11px] font-medium bg-gray-800 text-gray-400 border border-gray-700">Waiting for Run</span>'
    elif is_paused:
        progress_pct = 80
        progress_label = f"Stage 5 (Paused for Review • {pending_count} pending)"
        status_badge = f'<span id="pipeline-status-badge" class="px-2.5 py-0.5 rounded-full text-[11px] font-bold bg-amber-500/20 text-amber-300 border border-amber-500/40 animate-pulse"><span class="inline-block w-1.5 h-1.5 rounded-full bg-amber-400 animate-ping mr-1"></span> ⏸️ PAUSED: Human Review Required ({pending_count} items)</span>'
    elif is_running:
        progress_pct = int((active_step / 6) * 100)
        progress_label = f"Stage {active_step} of 6 ({progress_pct}%)"
        status_badge = f'<span id="pipeline-status-badge" class="px-2.5 py-0.5 rounded-full text-[11px] font-semibold bg-blue-500/20 text-blue-300 border border-blue-500/40 animate-pulse"><span class="inline-block w-1.5 h-1.5 rounded-full bg-blue-400 animate-ping mr-1"></span> Processing Stage {active_step}...</span>'
    else:
        progress_pct = 100
        progress_label = "100% (Completed)"
        status_badge = '<span id="pipeline-status-badge" class="px-2.5 py-0.5 rounded-full text-[11px] font-semibold bg-emerald-500/20 text-emerald-300 border border-emerald-500/30">✓ All 6 Stages Completed</span>'

    cards_html = []
    for s in stages_meta:
        num = s["num"]
        if not has_run:
            card_class = "border-gray-800 bg-gray-950/60 text-gray-500 opacity-60"
            badge_class = "bg-gray-800 text-gray-400"
            badge_html = "Waiting"
            icon_color = "text-gray-600"
        elif is_paused:
            if num < 5:
                card_class = "border-emerald-500/30 bg-emerald-950/25 text-emerald-200 shadow-sm"
                badge_class = "bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 font-semibold"
                badge_html = "✓ Done"
                icon_color = "text-emerald-400"
            elif num == 5:
                card_class = "border-2 border-amber-500 bg-amber-950/40 text-amber-100 ring-2 ring-amber-500/30 shadow-lg shadow-amber-500/20 transform scale-[1.02]"
                badge_class = "bg-amber-500/30 text-amber-200 border border-amber-400/50 font-bold animate-pulse"
                badge_html = '<span class="w-1.5 h-1.5 rounded-full bg-amber-400 animate-ping mr-1"></span> ⏸️ Paused'
                icon_color = "text-amber-400"
            else:
                card_class = "border-gray-800 bg-gray-950/60 text-gray-500 opacity-60"
                badge_class = "bg-gray-800 text-gray-400"
                badge_html = "Waiting for Resume"
                icon_color = "text-gray-600"
        elif is_running:
            if num < active_step:
                card_class = "border-emerald-500/30 bg-emerald-950/25 text-emerald-200 shadow-sm"
                badge_class = "bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 font-semibold"
                badge_html = "✓ Done"
                icon_color = "text-emerald-400"
            elif num == active_step:
                card_class = "border-2 border-blue-500 bg-blue-950/50 text-blue-100 ring-2 ring-blue-500/30 shadow-lg shadow-blue-500/20 transform scale-[1.02]"
                badge_class = "bg-blue-500/30 text-blue-200 border border-blue-400/50 font-bold animate-pulse"
                badge_html = '<span class="w-1.5 h-1.5 rounded-full bg-blue-400 animate-ping mr-1"></span> Active'
                icon_color = "text-blue-400"
            else:
                card_class = "border-gray-800 bg-gray-950/60 text-gray-500 opacity-60"
                badge_class = "bg-gray-800 text-gray-400"
                badge_html = "Waiting"
                icon_color = "text-gray-600"
        else:
            card_class = "border-emerald-500/30 bg-emerald-950/25 text-emerald-200 shadow-sm"
            badge_class = "bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 font-semibold"
            badge_html = "✓ Done"
            icon_color = "text-emerald-400"

        cards_html.append(f"""
        <div id="stage-card-{num}" class="stage-step-card p-3.5 rounded-xl border {card_class} transition-all duration-300 flex flex-col justify-between">
            <div>
                <div class="flex items-center justify-between mb-2">
                    <div class="flex items-center gap-1.5">
                        <svg class="w-3.5 h-3.5 {icon_color}" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="{s['icon']}"></path>
                        </svg>
                        <span class="text-[10px] font-bold uppercase tracking-wider text-gray-400">Stage {num}</span>
                    </div>
                    <span id="stage-badge-{num}" class="inline-flex items-center px-2 py-0.5 rounded-full text-[10px] {badge_class}">
                        {badge_html}
                    </span>
                </div>
                <div class="font-bold text-xs text-white mb-0.5">{s['title']}</div>
                <div class="text-[10px] text-blue-400 font-medium mb-1">{s['subtitle']}</div>
                <div class="text-[11px] text-gray-400 leading-snug">{s['desc']}</div>
            </div>
        </div>
        """)

    return f"""
    <div class="bg-gray-900 border border-gray-800 rounded-xl p-5 shadow-sm">
        <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 mb-3">
            <div>
                <div class="flex items-center gap-2">
                    <h3 class="text-sm font-semibold text-white tracking-wide">Migration Pipeline Stages</h3>
                    {status_badge}
                </div>
                <p class="text-xs text-gray-400 mt-0.5">Simple, automated step-by-step progress from raw files to verified Darwinbox employee records.</p>
            </div>
            <div class="text-xs font-mono text-gray-400 bg-gray-950 px-3 py-1.5 rounded-lg border border-gray-800 flex items-center gap-2 self-start sm:self-auto">
                <span class="text-gray-500">Progress:</span>
                <span id="pipeline-progress-text" class="text-emerald-400 font-bold">{progress_label}</span>
            </div>
        </div>

        <div class="w-full bg-gray-800 rounded-full h-1.5 mb-4 overflow-hidden shadow-inner">
            <div id="pipeline-progress-bar" class="bg-gradient-to-r from-emerald-500 via-blue-500 to-indigo-500 h-1.5 rounded-full transition-all duration-500" style="width: {progress_pct}%;"></div>
        </div>

        <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3" id="pipeline-stage-cards">
            {''.join(cards_html)}
        </div>
    </div>
    """


def render_inline_escalations_html(run_id: str, escalations: list) -> str:
    """Renders the prominent in-page Human Review Panel when the pipeline is paused."""
    if not escalations:
        return ""

    cards_html = []
    for esc in escalations:
        esc_id = esc["id"]
        esc_type = esc["type"]
        context = esc["context"]
        proposal = esc.get("proposal") or {}
        affected_count = esc.get("affected_count", 0)

        # Evidence pill tags
        samples_html = "".join([
            f'<span class="px-2 py-0.5 rounded bg-gray-800 text-gray-200 font-mono text-[11px]">{s}</span>'
            for s in context.get("sample_values", [])
        ]) or '<span class="text-gray-500 italic">No sample values available</span>'

        # Proposal display
        suggested_val = proposal.get("target_field") or proposal.get("canonical_value") or proposal.get("action") or "Review required"
        score_val = f'Calculated score: {proposal["confidence"]:.2f}' if "confidence" in proposal else ""
        rationale_val = f'"{proposal["rationale"]}"' if "rationale" in proposal else ""

        cards_html.append(f"""
        <div id="esc-card-{esc_id}" class="bg-gray-900 border border-amber-900/60 rounded-xl p-4 shadow-sm space-y-3">
            <div class="flex items-start justify-between gap-3">
                <div class="space-y-1">
                    <div class="flex items-center gap-2">
                        <span class="px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider bg-amber-500/20 text-amber-300 border border-amber-500/30">
                            {esc_type}
                        </span>
                        <span class="text-[10px] text-gray-400 font-mono">Group: {esc['group_key']}</span>
                    </div>
                    <h4 class="text-xs font-bold text-white">{context.get('title', esc_type)}</h4>
                </div>
                <div class="text-[11px] text-gray-400 font-medium whitespace-nowrap">
                    Impact: <span class="text-amber-300 font-semibold">{affected_count} records</span>
                </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-2 gap-3 bg-gray-950/80 p-3 rounded-lg border border-gray-800 text-xs">
                <div>
                    <span class="text-gray-400 font-semibold block mb-1 text-[10px] uppercase tracking-wider">Observable Evidence:</span>
                    <div class="flex flex-wrap gap-1">
                        {samples_html}
                    </div>
                </div>
                <div>
                    <span class="text-gray-400 font-semibold block mb-1 text-[10px] uppercase tracking-wider">AI Proposed Action:</span>
                    <div class="p-2 rounded bg-blue-950/40 border border-blue-800/40 text-blue-200 text-xs">
                        <div class="font-semibold text-white">Suggested: {suggested_val}</div>
                        {f'<div class="text-blue-300 text-[10px] mt-0.5">{score_val}</div>' if score_val else ''}
                        {f'<div class="text-gray-300 text-[10px] mt-0.5 italic">{rationale_val}</div>' if rationale_val else ''}
                    </div>
                </div>
            </div>

            <form 
                hx-post="/api/escalations/{esc_id}/resolve" 
                hx-target="#esc-card-{esc_id}" 
                hx-swap="outerHTML"
                class="flex flex-wrap items-center justify-between gap-2 pt-2 border-t border-gray-800 text-xs">
                <div class="flex items-center gap-3 text-gray-300">
                    <label class="flex items-center gap-1.5 cursor-pointer text-[11px]">
                        <input type="checkbox" name="apply_to_group" checked class="rounded bg-gray-800 border-gray-700 text-blue-600 focus:ring-0">
                        Apply to all {affected_count} rows
                    </label>
                    <label class="flex items-center gap-1.5 cursor-pointer text-[11px]">
                        <input type="checkbox" name="remember" checked class="rounded bg-gray-800 border-gray-700 text-blue-600 focus:ring-0">
                        Remember as rule
                    </label>
                </div>

                <div class="flex items-center gap-2">
                    <button 
                        type="submit" 
                        name="action" 
                        value="approve" 
                        class="px-3 py-1.5 bg-emerald-600 hover:bg-emerald-500 text-white font-semibold text-xs rounded-lg transition active:scale-95 shadow-sm cursor-pointer">
                        ✓ Approve AI Proposal
                    </button>
                    <button 
                        type="submit" 
                        name="action" 
                        value="reject" 
                        class="px-3 py-1.5 bg-red-950/40 hover:bg-red-900/60 border border-red-800/50 text-red-300 font-semibold text-xs rounded-lg transition active:scale-95 cursor-pointer">
                        Reject
                    </button>
                </div>
            </form>
        </div>
        """)

    return f"""
    <!-- Human Intervention Panel (Inline on Dashboard) -->
    <div class="bg-amber-950/20 border-2 border-amber-500/60 rounded-xl p-5 shadow-lg shadow-amber-500/5 space-y-4" id="human-intervention-panel">
        <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-3 border-b border-amber-800/40">
            <div class="flex items-center gap-3">
                <span class="flex h-3 w-3 relative">
                    <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-amber-400 opacity-75"></span>
                    <span class="relative inline-flex rounded-full h-3 w-3 bg-amber-500"></span>
                </span>
                <div>
                    <h3 class="text-sm font-bold text-amber-200 tracking-wide uppercase">
                        ⏸️ Human Intervention Required — Pipeline Paused
                    </h3>
                    <p class="text-xs text-amber-300/80 mt-0.5">
                        The agent paused before target push. Review & approve decisions below, then click Resume to push verified records.
                    </p>
                </div>
            </div>

            <button 
                hx-post="/api/pipeline/resume/{run_id}"
                hx-target="#run-container"
                hx-swap="outerHTML"
                class="px-4 py-2 bg-gradient-to-r from-emerald-600 to-teal-600 hover:from-emerald-500 hover:to-teal-500 text-white font-bold text-xs rounded-lg shadow-md shadow-emerald-500/20 flex items-center gap-2 transition active:scale-95 cursor-pointer whitespace-nowrap">
                <svg class="w-4 h-4" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM9.555 7.168A1 1 0 008 8v4a1 1 0 001.555.832l3-2a1 1 0 000-1.664l-3-2z" clip-rule="evenodd"></path></svg>
                <span>▶️ Resume Pipeline &amp; Push to Darwinbox</span>
            </button>
        </div>

        <div class="space-y-3" id="inline-escalations-list">
            {''.join(cards_html)}
        </div>
    </div>
    """


@app.get("/", response_class=HTMLResponse)
async def index_view(request: Request):
    conn = get_db_connection()
    run_id = get_latest_run_id(conn)
    stats = get_dashboard_stats(conn, run_id)
    events = tail_events(conn, run_id, last_id=0, limit=50) if run_id else []

    run_status = "completed"
    if run_id:
        row = conn.execute("SELECT status FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row:
            run_status = row["status"]

    pending_escs = get_pending_escalations(conn, run_id) if run_id else []
    is_paused = (run_status == "paused") or (len(pending_escs) > 0 and run_id != "No Active Run")

    stages_html = render_stages_html(
        run_id or "No Active Run",
        is_paused=is_paused,
        pending_count=len(pending_escs),
    )
    inline_escalations_html = render_inline_escalations_html(run_id or "", pending_escs) if is_paused else ""

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "run_id": run_id or "No Active Run",
            "stats": stats,
            "events": events,
            "stages_html": stages_html,
            "inline_escalations_html": inline_escalations_html,
        },
    )


def render_run_container_html(
    run_id: str,
    stats: dict,
    events: list,
    banner_msg: Optional[str] = None,
    is_running: bool = False,
    active_step: int = 6,
    is_paused: bool = False,
    pending_escalations: Optional[list] = None,
) -> str:
    """Helper to render the reactive #run-container partial."""
    banner_html = ""
    if banner_msg:
        banner_bg = "bg-amber-950/60 border-amber-800/60 text-amber-200" if is_paused else "bg-blue-950/60 border-blue-800/60 text-blue-200"
        dot_bg = "bg-amber-400" if is_paused else "bg-blue-400"
        banner_html = f"""
        <div class="p-3.5 rounded-lg {banner_bg} border text-xs flex items-center justify-between shadow-sm">
            <span class="flex items-center gap-2">
                <span class="h-2 w-2 rounded-full {dot_bg}"></span>
                <span>{banner_msg}</span>
            </span>
            <span class="text-blue-400 font-mono text-[11px]">Run: {run_id}</span>
        </div>
        """

    rendered_events = "".join([
        f"""<div class="p-2.5 rounded-lg bg-gray-950/80 border border-gray-800/80 flex items-start gap-3 hover:border-gray-700 transition" data-stage="{ev['stage']}" data-type="{ev['type']}">
            <span class="text-gray-500 whitespace-nowrap">{ev['ts'][11:19]}</span>
            <span class="px-1.5 py-0.5 rounded text-[10px] font-semibold {'bg-blue-500/20 text-blue-300 border border-blue-500/30' if ev['actor'] == 'agent' else 'bg-amber-500/20 text-amber-300 border border-amber-500/30'}">{ev['actor'].upper()}</span>
            <span class="px-1.5 py-0.5 rounded text-[10px] font-bold bg-gray-800 text-gray-300">{ev['type']}</span>
            <span class="text-gray-300 flex-1 break-words font-sans">{ev['reason'] or ev['type']}</span>
        </div>"""
        for ev in events
    ]) or """<div class="text-center py-10 text-gray-500 font-sans text-sm">No events recorded. Upload new files or run sample fixtures to begin.</div>"""

    if pending_escalations is None and run_id and run_id != "No Active Run":
        conn = get_db_connection()
        pending_escalations = get_pending_escalations(conn, run_id)
        if len(pending_escalations) > 0 and not is_paused:
            row = conn.execute("SELECT status FROM runs WHERE id = ?", (run_id,)).fetchone()
            if row and row["status"] == "paused":
                is_paused = True

    sse_connect_attr = f'sse-connect="/api/events/stream/{run_id}"' if run_id and run_id != "No Active Run" else ''
    stages_component = render_stages_html(
        run_id,
        is_running=is_running,
        active_step=active_step,
        is_paused=is_paused,
        pending_count=len(pending_escalations or []),
    )
    escalations_panel = render_inline_escalations_html(run_id, pending_escalations or []) if is_paused else ""

    return f"""
    <div id="run-container" class="space-y-6">
        {banner_html}
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div class="bg-gray-900 border border-gray-800 rounded-xl p-4">
                <div class="text-xs font-medium text-gray-400 uppercase tracking-wider">Source Records In</div>
                <div class="text-2xl font-bold text-white mt-1">{stats['total_source_rows']}</div>
                <div class="text-xs text-gray-500 mt-1">Processed rows</div>
            </div>
            <div class="bg-gray-900 border border-gray-800 rounded-xl p-4">
                <div class="text-xs font-medium text-gray-400 uppercase tracking-wider">Autonomy Score</div>
                <div class="text-2xl font-bold text-emerald-400 mt-1">{stats['autonomy_score']}%</div>
                <div class="text-xs text-gray-500 mt-1">Automated resolutions</div>
            </div>
            <div class="bg-gray-900 border border-gray-800 rounded-xl p-4">
                <div class="text-xs font-medium text-gray-400 uppercase tracking-wider">Pending Escalations</div>
                <div class="text-2xl font-bold text-amber-400 mt-1">{stats['pending_escalations']}</div>
                <div class="text-xs text-gray-500 mt-1">Requires human review</div>
            </div>
            <div class="bg-gray-900 border border-gray-800 rounded-xl p-4">
                <div class="text-xs font-medium text-gray-400 uppercase tracking-wider">Pushed to Darwinbox</div>
                <div class="text-2xl font-bold text-blue-400 mt-1">{stats['pushed_records']}</div>
                <div class="text-xs text-gray-500 mt-1">Target API verified</div>
            </div>
        </div>

        {stages_component}

        {escalations_panel}

        <div class="bg-gray-900 border border-gray-800 rounded-xl p-5 shadow-sm">
            <div class="flex items-center justify-between pb-3 border-b border-gray-800 mb-4">
                <div class="flex items-center gap-2">
                    <span class="relative flex h-2.5 w-2.5">
                        <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                        <span class="relative inline-flex rounded-full h-2.5 w-2.5 bg-emerald-500"></span>
                    </span>
                    <h3 class="text-sm font-semibold text-white">Append-Only Event Stream</h3>
                </div>
                <span class="text-xs text-gray-500 font-mono">Run: {run_id}</span>
            </div>
            <div hx-ext="sse" {sse_connect_attr} sse-swap="event" class="space-y-2.5 max-h-[480px] overflow-y-auto custom-scroll pr-1 font-mono text-xs" id="events-feed">
                {rendered_events}
            </div>
        </div>
    </div>
    """


@app.post("/api/run", response_class=HTMLResponse)
async def trigger_run(request: Request):
    """Trigger a new migration run with default sample fixtures."""
    result = await asyncio.to_thread(run_pipeline, pause_on_escalation=True)
    run_id = result["run_id"]
    conn = get_db_connection()
    stats = get_dashboard_stats(conn, run_id)
    events = tail_events(conn, run_id, last_id=0, limit=50)
    is_paused = (result.get("status") == "paused")
    pending_escs = get_pending_escalations(conn, run_id) if is_paused else []
    banner_msg = f"Pipeline paused: {len(pending_escs)} items require human review before pushing to Darwinbox." if is_paused else "Pipeline executed successfully using sample multi-file fixtures."
    return HTMLResponse(render_run_container_html(
        run_id, stats, events, banner_msg=banner_msg, is_paused=is_paused, pending_escalations=pending_escs
    ))


@app.post("/api/upload-and-run", response_class=HTMLResponse)
async def upload_and_run(files: List[UploadFile] = File(...)):
    """Accept user-uploaded CSV / Excel files and run the migration pipeline on them."""
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    saved_paths = []
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    for f in files:
        if not f.filename:
            continue
        clean_name = Path(f.filename).name
        dest = UPLOAD_DIR / f"{ts}_{clean_name}"
        content = await f.read()
        dest.write_bytes(content)
        saved_paths.append(dest)

    if not saved_paths:
        raise HTTPException(status_code=400, detail="No valid files provided")

    result = await asyncio.to_thread(run_pipeline, file_paths=saved_paths, pause_on_escalation=True)
    run_id = result["run_id"]
    conn = get_db_connection()
    stats = get_dashboard_stats(conn, run_id)
    events = tail_events(conn, run_id, last_id=0, limit=50)
    is_paused = (result.get("status") == "paused")
    pending_escs = get_pending_escalations(conn, run_id) if is_paused else []
    banner_msg = f"Pipeline paused: {len(pending_escs)} items require human review before pushing to Darwinbox." if is_paused else f"Successfully ingested and processed {len(saved_paths)} uploaded file(s)."
    return HTMLResponse(render_run_container_html(
        run_id,
        stats,
        events,
        banner_msg=banner_msg,
        is_paused=is_paused,
        pending_escalations=pending_escs,
    ))


@app.post("/api/pipeline/resume/{run_id}", response_class=HTMLResponse)
async def resume_pipeline_endpoint(run_id: str):
    """Resume a paused pipeline run after human review and push records to target API."""
    result = await asyncio.to_thread(resume_pipeline, run_id=run_id)
    conn = get_db_connection()
    stats = get_dashboard_stats(conn, run_id)
    events = tail_events(conn, run_id, last_id=0, limit=50)
    return HTMLResponse(render_run_container_html(
        run_id,
        stats,
        events,
        banner_msg=f"Pipeline resumed! Successfully completed push to Darwinbox API ({result.get('valid_records', 0)} records pushed).",
        is_paused=False,
    ))


@app.post("/api/db/reset", response_class=HTMLResponse)
async def reset_db_endpoint():
    """Resets the entire database and returns empty dashboard."""
    reset_database()
    empty_stats = {
        "total_source_rows": 0,
        "autonomy_score": 100.0,
        "pending_escalations": 0,
        "pushed_records": 0,
    }
    return HTMLResponse(render_run_container_html(
        "No Active Run",
        empty_stats,
        [],
        banner_msg="Database reset complete! All previous runs, events, escalations, and rules have been cleared.",
    ))


@app.get("/api/events/stream/{run_id}")
async def stream_events(run_id: str, from_id: Optional[int] = None):
    """Server-Sent Events endpoint streaming pipeline events live."""
    async def event_generator():
        conn = get_db_connection()
        if from_id is not None:
            last_id = from_id
        else:
            row = conn.execute("SELECT MAX(id) as max_id FROM events WHERE run_id = ?", (run_id,)).fetchone()
            last_id = (row["max_id"] or 0) if row else 0

        while True:
            new_events = tail_events(conn, run_id, last_id=last_id, limit=20)
            if new_events:
                for ev in new_events:
                    last_id = ev["id"]
                    actor_badge = (
                        '<span class="px-1.5 py-0.5 rounded text-[10px] font-semibold bg-blue-500/20 text-blue-300 border border-blue-500/30">AGENT</span>'
                        if ev['actor'] == 'agent'
                        else '<span class="px-1.5 py-0.5 rounded text-[10px] font-semibold bg-amber-500/20 text-amber-300 border border-amber-500/30">HUMAN</span>'
                    )
                    score_span = f'<span class="text-emerald-400 font-semibold whitespace-nowrap">score: {ev["score"]:.2f}</span>' if ev["score"] else ''
                    html_item = (
                        f'<div class="p-2.5 rounded-lg bg-gray-950/80 border border-gray-800/80 flex items-start gap-3 hover:border-gray-700 transition" data-stage="{ev["stage"]}" data-type="{ev["type"]}">'
                        f'<span class="text-gray-500 whitespace-nowrap">{ev["ts"][11:19]}</span>'
                        f'{actor_badge}'
                        f'<span class="px-1.5 py-0.5 rounded text-[10px] font-bold bg-gray-800 text-gray-300">{ev["type"]}</span>'
                        f'<span class="text-gray-300 flex-1 break-words font-sans">{ev["reason"] or ev["type"]}</span>'
                        f'{score_span}'
                        f'</div>'
                    )
                    yield f"event: event\ndata: {html_item}\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/queue", response_class=HTMLResponse)
async def queue_view(request: Request):
    """Review queue showing open escalation cards."""
    conn = get_db_connection()
    run_id = get_latest_run_id(conn)
    run_status = None
    if run_id:
        row = conn.execute("SELECT status FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row:
            run_status = row["status"]
    is_paused = (run_status == "paused")

    escalations_raw = conn.execute(
        "SELECT * FROM escalations ORDER BY status ASC, created_at DESC"
    ).fetchall()

    escalations = []
    for row in escalations_raw:
        context = json.loads(row["context_json"])
        proposal = json.loads(row["proposal_json"]) if row["proposal_json"] else None
        affected = json.loads(row["affected_ids_json"] or "[]")
        res_data = json.loads(row["resolution_json"]) if row["resolution_json"] else None

        escalations.append({
            "id": row["id"],
            "type": row["type"],
            "group_key": row["group_key"],
            "status": row["status"],
            "context": context,
            "proposal": proposal,
            "affected_count": len(affected),
            "created_at": row["created_at"],
            "resolution": res_data,
        })

    return templates.TemplateResponse(
        request=request,
        name="queue.html",
        context={
            "run_id": run_id,
            "is_paused": is_paused,
            "escalations": escalations,
        },
    )


@app.post("/api/escalations/{esc_id}/resolve", response_class=HTMLResponse)
async def resolve_escalation_endpoint(
    esc_id: str,
    action: str = Form(...),
    correct_value: Optional[str] = Form(None),
    remember: bool = Form(False),
):
    """Resolve an escalation via UI form."""
    conn = get_db_connection()
    result = await asyncio.to_thread(
        resolve_escalation,
        conn=conn,
        escalation_id=esc_id,
        action=action,
        value=correct_value if action == "correct" else None,
        remember=remember,
    )

    # Return updated resolved card partial
    return HTMLResponse(f"""
    <div id="esc-card-{esc_id}" class="bg-gray-950 border border-emerald-900/60 rounded-xl p-5 shadow-sm space-y-2 opacity-80">
        <div class="flex items-center justify-between">
            <div class="flex items-center gap-2">
                <span class="px-2 py-0.5 rounded text-xs font-bold uppercase tracking-wider bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">
                    ✓ Resolved ({action.upper()})
                </span>
                <span class="text-xs text-gray-400 font-mono">ID: {esc_id[:8]}</span>
            </div>
            <span class="text-xs text-gray-500">Just now</span>
        </div>
        <p class="text-xs text-gray-300 font-sans">
            Action: <strong>{action}</strong> {f'- Value: {correct_value}' if correct_value else ''} 
            {'• Saved to client resolution memory' if remember else ''}
        </p>
    </div>
    """)


@app.get("/report", response_class=HTMLResponse)
async def report_view(request: Request, run_id: Optional[str] = None):
    """Reconciliation report and autonomy scoreboard."""
    conn = get_db_connection()
    if not run_id:
        run_id = get_latest_run_id(conn)
    report = generate_reconciliation_report(conn, run_id)
    return templates.TemplateResponse(
        request=request,
        name="report.html",
        context={
            "report": report,
        },
    )


@app.get("/audit", response_class=HTMLResponse)
async def audit_view(request: Request, run_id: Optional[str] = None, actor: Optional[str] = None):
    """Full audit log and event ledger view with filtering."""
    conn = get_db_connection()
    if not run_id:
        run_id = get_latest_run_id(conn)

    query = "SELECT * FROM events WHERE run_id = ?"
    params = [run_id]
    if actor:
        query += " AND actor = ?"
        params.append(actor)
    query += " ORDER BY id DESC LIMIT 500"

    events = [dict(r) for r in conn.execute(query, params).fetchall()]
    total_events = conn.execute("SELECT COUNT(*) as cnt FROM events WHERE run_id = ?", (run_id,)).fetchone()["cnt"]

    return templates.TemplateResponse(
        request=request,
        name="audit.html",
        context={
            "run_id": run_id,
            "events": events,
            "total_events": total_events,
            "current_actor": actor,
        },
    )


@app.get("/api/audit/export.json")
async def export_audit_json(run_id: Optional[str] = None):
    """Export audit log as JSON."""
    conn = get_db_connection()
    if not run_id:
        run_id = get_latest_run_id(conn)
    rows = conn.execute("SELECT * FROM events WHERE run_id = ? ORDER BY id ASC", (run_id,)).fetchall()
    data = [dict(r) for r in rows]
    return JSONResponse(
        content=data,
        headers={"Content-Disposition": f'attachment; filename="audit_log_{run_id}.json"'},
    )


@app.get("/api/audit/export.csv")
async def export_audit_csv(run_id: Optional[str] = None):
    """Export audit log as CSV."""
    import csv
    import io
    conn = get_db_connection()
    if not run_id:
        run_id = get_latest_run_id(conn)
    rows = conn.execute("SELECT * FROM events WHERE run_id = ? ORDER BY id ASC", (run_id,)).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "run_id", "ts", "stage", "type", "actor", "entity_type", "entity_id", "field", "before", "after", "reason", "score"])
    for r in rows:
        writer.writerow([r["id"], r["run_id"], r["ts"], r["stage"], r["type"], r["actor"], r["entity_type"], r["entity_id"], r["field"], r["before"], r["after"], r["reason"], r["score"]])

    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="audit_log_{run_id}.csv"'},
    )


@app.get("/records", response_class=HTMLResponse)
async def records_view(request: Request, run_id: Optional[str] = None):
    """View structured canonical records and Darwinbox push statuses."""
    conn = get_db_connection()
    if not run_id:
        run_id = get_latest_run_id(conn)

    records = []
    pushed_count = 0

    if run_id:
        can_rows = conn.execute(
            """
            SELECT c.*, p.http_status, p.idempotency_key, p.ts as push_ts, p.error as push_error
            FROM canonical_records c
            LEFT JOIN (
                SELECT record_id, MAX(id) as max_p_id
                FROM push_attempts
                WHERE run_id = ?
                GROUP BY record_id
            ) latest_p ON c.canonical_key = latest_p.record_id
            LEFT JOIN push_attempts p ON latest_p.max_p_id = p.id
            WHERE c.run_id = ?
            ORDER BY c.canonical_key ASC
            """,
            (run_id, run_id),
        ).fetchall()

        # Map raw rows for side-by-side comparison
        raw_rows = conn.execute("SELECT * FROM source_rows WHERE run_id = ?", (run_id,)).fetchall()
        raw_map = {}
        for r in raw_rows:
            try:
                raw_dict = json.loads(r["raw_json"])
                for k, v in raw_dict.items():
                    if any(id_w in k.lower() for id_w in ["id", "emp", "no", "code"]):
                        raw_map[str(v).strip()] = raw_dict
            except Exception:
                pass

        for row in can_rows:
            try:
                data = json.loads(row["data_json"])
            except Exception:
                data = {}
            try:
                provenance = json.loads(row["provenance_json"]) if row["provenance_json"] else {}
            except Exception:
                provenance = {}

            key = row["canonical_key"]
            raw_data = raw_map.get(key)
            if not raw_data:
                for r in raw_rows:
                    if key in r["raw_json"]:
                        try:
                            raw_data = json.loads(r["raw_json"])
                            break
                        except Exception:
                            pass

            if row["status"] == "pushed":
                pushed_count += 1

            records.append({
                "canonical_key": key,
                "status": row["status"],
                "data": data,
                "provenance": provenance,
                "raw_data": raw_data,
                "http_status": row["http_status"],
                "idempotency_key": row["idempotency_key"],
                "push_ts": row["push_ts"],
            })

    total_count = len(records)
    sync_rate = round((pushed_count / total_count * 100)) if total_count > 0 else 0

    return templates.TemplateResponse(
        request=request,
        name="records.html",
        context={
            "run_id": run_id,
            "records": records,
            "pushed_count": pushed_count,
            "sync_rate": sync_rate,
        },
    )


@app.get("/api/export/records.csv")
async def export_records_csv(run_id: Optional[str] = None):
    """Export canonical records as CSV."""
    import csv
    import io
    conn = get_db_connection()
    if not run_id:
        run_id = get_latest_run_id(conn)
    rows = conn.execute("SELECT canonical_key, data_json, status FROM canonical_records WHERE run_id = ? ORDER BY canonical_key ASC", (run_id,)).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["employee_id", "first_name", "last_name", "email", "phone", "date_of_birth", "hire_date", "department", "job_title", "employment_status", "salary", "push_status"])
    for r in rows:
        try:
            d = json.loads(r["data_json"])
        except Exception:
            d = {}
        writer.writerow([
            r["canonical_key"],
            d.get("first_name", ""),
            d.get("last_name", ""),
            d.get("email", ""),
            d.get("phone", ""),
            d.get("date_of_birth", ""),
            d.get("hire_date", ""),
            d.get("department", ""),
            d.get("job_title", ""),
            d.get("employment_status", ""),
            d.get("salary", ""),
            r["status"],
        ])

    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="darwinbox_records_{run_id}.csv"'},
    )


@app.get("/api/export/records.json")
async def export_records_json(run_id: Optional[str] = None):
    """Export canonical records as JSON."""
    conn = get_db_connection()
    if not run_id:
        run_id = get_latest_run_id(conn)
    rows = conn.execute("SELECT canonical_key, data_json, status FROM canonical_records WHERE run_id = ? ORDER BY canonical_key ASC", (run_id,)).fetchall()

    records = []
    for r in rows:
        try:
            d = json.loads(r["data_json"])
        except Exception:
            d = {}
        records.append({"canonical_key": r["canonical_key"], "status": r["status"], "record": d})

    return Response(
        content=json.dumps({"run_id": run_id, "total": len(records), "records": records}, indent=2),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="darwinbox_records_{run_id}.json"'},
    )


@app.get("/rules", response_class=HTMLResponse)
async def rules_view(request: Request, client_id: str = "acme_corp"):
    """Learned rules and resolution memory view."""
    conn = get_db_connection()
    rows = conn.execute(
        "SELECT * FROM rules WHERE client_id = ? ORDER BY created_at DESC",
        (client_id,),
    ).fetchall()

    rules = []
    for r in rows:
        action_data = json.loads(r["action_json"])
        if r["kind"] == "mapping":
            display = f"Map to '{action_data.get('target_field')}'"
        elif r["kind"] == "enum_value":
            display = f"Set enum value '{action_data.get('canonical_value')}'"
        elif r["kind"] == "date_format":
            display = f"Apply format '{action_data.get('date_format')}'"
        elif r["kind"] == "sensitive_mapping_confirmed":
            display = "Confirmed sensitive mapping approved"
        else:
            display = str(action_data)

        rules.append({
            "id": r["id"],
            "client_id": r["client_id"],
            "kind": r["kind"],
            "fingerprint": r["fingerprint"],
            "action_display": display,
            "times_applied": r["times_applied"],
            "enabled": bool(r["enabled"]),
            "created_at": r["created_at"],
        })

    return templates.TemplateResponse(
        request=request,
        name="rules.html",
        context={
            "client_id": client_id,
            "rules": rules,
        },
    )


@app.post("/api/rules/{rule_id}/toggle", response_class=HTMLResponse)
async def toggle_rule_endpoint(rule_id: str):
    """Toggle enabled/disabled state of a learned rule."""
    conn = get_db_connection()
    with conn:
        conn.execute("UPDATE rules SET enabled = 1 - enabled WHERE id = ?", (rule_id,))
    row = conn.execute("SELECT enabled FROM rules WHERE id = ?", (rule_id,)).fetchone()
    enabled = bool(row["enabled"]) if row else False

    status_text = "● Active" if enabled else "○ Disabled"
    color_classes = (
        "bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 hover:bg-emerald-500/30"
        if enabled
        else "bg-gray-800 text-gray-400 border border-gray-700 hover:text-white"
    )
    return HTMLResponse(f"""
    <button 
        hx-post="/api/rules/{rule_id}/toggle"
        hx-swap="outerHTML"
        class="px-3 py-1 rounded text-xs font-semibold transition cursor-pointer {color_classes}">
        {status_text}
    </button>
    """)


@app.post("/api/run/rollback")
async def rollback_run_endpoint(run_id: Optional[str] = None):
    """Manual compensating rollback of an entire run."""
    conn = get_db_connection()
    if not run_id:
        run_id = get_latest_run_id(conn)
    result = rollback_run(conn, run_id)
    return result
