# Implementation Plan: AI Agent for Client Data Migration

This document outlines the step-by-step implementation plan for building an autonomous AI agent to handle client data migrations.

## 1. Architecture Overview
- **Event Log Driven**: Every action (ingest, map, normalize, validate, escalate, human-decision) is an append-only event. This provides a free audit trail and simplifies state management.
- **LLM Boundaries**: The LLM is strictly used for proposing field mappings and normalizing fuzzy values (e.g., job titles). Deterministic Python code handles validation, deduplication, and pipeline execution.
- **Escalation Boundary**:
  - *Automated*: Confidence >= 0.85, straightforward normalizations, exact duplicate matches.
  - *Escalated*: Confidence < 0.85, ambiguous top choices, sensitive fields (salary, ID), repeated validation failures, fuzzy duplicate similarities.
- **Circuit Breaker**: If >15% of rows require escalation, halt the run and escalate the job itself to prevent overwhelming the human reviewer.

## 2. Technology Stack
- **Backend Core**: Python, FastAPI
- **Database / Event Store**: SQLite (for rapid prototyping and simple state management)
- **Real-time Updates**: Server-Sent Events (SSE) for live stream to the UI
- **Frontend / UI**: React (or HTMX) with TailwindCSS for a clean, responsive Human-In-The-Loop interface
- **AI / LLM Runtime**: Ollama running locally (Qwen or Llama 8B) to satisfy the open-source constraint while remaining highly performant.

## 3. Implementation Phases

### Phase 0: Contract & Fixtures
- Define target schema (YAML) with required, sensitive, and enum annotations.
- Create 3 mocked source data files (CSV/Excel) containing:
  - Different column names
  - Ambiguous dates
  - Near-duplicates
  - Missing fields
- Formalize `DECISIONS.md` to document the escalation matrix.

### Phase 1: Deterministic Core (No LLM, No UI)
- Build file ingestion pipeline (Pandas/Polars).
- Implement column profiling (type inference, null rate).
- Build basic deterministic validation, normalization, and exact deduplication.
- Establish the Event Log system to record all pipeline actions.
- Execute via CLI.

### Phase 2: Mapping Agent + Confidence Policy
- Integrate Ollama for LLM-based column mapping proposals.
- Implement the confidence thresholds and sensitive field carve-outs.
- Model escalations as first-class objects linked to specific data rows or columns.
- Ensure human resolutions correctly trigger a re-run of the pipeline for affected records.

### Phase 3: Mock Target Integration + Push
- Build stub target API endpoints.
- Implement the "Push" phase: send validated/cleaned records to the stub API.
- Add retry logic with exponential backoff for 5xx errors.
- Add rollback logic for batch failures.
- Record all API results in the Event Log.

### Phase 4: Human-in-the-Loop UI
- **Live Run Screen**: Render the real-time event stream and processing counters via SSE.
- **Escalation Queue**: Cards showing the ambiguity, the LLM's rationale/confidence, and clear actions (Approve / Correct / Reject).
- **Audit Log**: A historical view of before/after states for all data.

### Phase 5: Packaging & Demo
- Finalize `README.md` with local setup instructions.
- Record a short video demonstrating an end-to-end run, specifically highlighting the resolution of an escalation via the UI.
