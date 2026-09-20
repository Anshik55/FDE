# Autonomy & Escalation Decisions

> **Core Guiding Principle:**
> *Auto-handle what is reversible, verifiable by code, and low-stakes. Escalate what is ambiguous, sensitive, or irreversible.*

This document defends the autonomy boundary implemented by the AI Agent for Client Data Migration.

---

## 1. The Escalation Boundary Matrix

| Situation | Action | Rationale |
|---|---|---|
| **Whitespace, casing, phone format** | **Auto-fix** | Completely reversible and deterministically verifiable. Names are re-cased only if ALL-CAPS or all-lowercase (to preserve casing like *McDonald* or *O'Brien*). Phones are standardized to E.164. |
| **Date column: at least one day > 12** | **Auto-resolve (DD/MM)** | A single value like `24/03/2022` provides unambiguous mathematical proof of the column-level date convention. The whole column is normalized deterministically. |
| **Date column: all days ≤ 12** | **Escalate** | Inherently ambiguous (e.g. `03/04/2022` could be March 4th or April 3rd). A wrong guess permanently and silently corrupts historical HR records. Raised once per column, not per row. |
| **Column name match (high score & margin)** | **Auto-map** | Score $\ge 0.75$ and margin over runner-up $\ge 0.20$ based on multi-factor evidence (name similarity, type parse rate, enum hit rate, patterns). |
| **Column name ambiguity (top-2 margin < 0.20)** | **Escalate** | Column could plausibly map to two target fields (e.g. `ref` matching both `employee_id` and `manager_id`). Surfaced with side-by-side sample values. |
| **Sensitive column mapping** | **Confirm Once (Batched)** | Target fields tagged `sensitive: true` (e.g. `salary`, `date_of_birth`) require one-click human confirmation per run. Remembered across runs. |
| **Unmapped source columns with no target** | **Drop & Log** | Raw source files are retained in `source_rows` untouched; dropped columns are listed in the final reconciliation report. |
| **Enum matching synonym map / fuzzy $\ge 0.90$** | **Auto-normalize** | Checked deterministically against target enum synonym dictionaries. |
| **Unrecognized enum value** | **Escalate (with LLM proposal)** | Surfaced to human with LLM's best proposal (e.g. `"LOA"` $\rightarrow$ `"On Leave"`). Once confirmed, saved as a client-scoped rule so all identical future values resolve automatically. |
| **Exact duplicate records** | **Auto-merge** | Same identity key (e.g. `employee_id` or `email`) with no conflicting data. Missing values in one source are backfilled from the other. |
| **Non-sensitive field conflict** | **Auto-resolve (Precedence)** | e.g. Job title differs between HRIS and CRM. Follows configured `source_precedence` (HRIS > CRM > Payroll) and is logged in the reconciliation report for spot checks. |
| **Sensitive field conflict (e.g. salary, DOB)** | **Escalate** | Discrepancies in payroll vs HRIS compensation cannot be decided by a heuristic without legal/client repercussions. |
| **Near-duplicate records** | **Escalate** | High name similarity ($\ge 90$) and identical DOB, but differing employee ID or email (e.g., "Rahul Sharma" vs "Rahul Sherma"). Merging two different individuals is irreversible in the target system. |
| **Validation failure $\rightarrow$ auto-fix $\rightarrow$ second failure** | **Escalate** | Implements the literal "fails validation twice" criteria. For example, a missing required email cannot be filled from any source. |
| **Push 5xx / Network Timeout** | **Auto-retry** | Handled with exponential backoff and jitter (up to 3 attempts). |
| **Push 4xx Client Error** | **Escalate & Park** | The target API explicitly rejected the payload (e.g., schema violation or rejected department). Never retried blindly. |
| **Target API Outage (repeated 5xx failures)** | **Batch Rollback & Escalate** | Outage circuit breaker triggers compensating `DELETE` requests for the partially pushed batch to prevent dirty partial state. |

---

## 2. Architectural Guardrails: Why This Shows Judgment

### 2.1 Evidence Over Self-Reported LLM Confidence
Small open-source language models (7B/8B) are notoriously miscalibrated when self-reporting confidence (e.g. hallucinating `0.95` confidence on wrong guesses). We compute deterministic confidence scores from observable evidence:
- Syntactic string distance (RapidFuzz token sort)
- Target type parse rate across column values
- Target enum synonym overlap
- Regular expression pattern adherence (e.g. `^E\d{4}$`)
- Column uniqueness and referential integrity

The LLM is invoked only for tie-breaking or unstructured enum mapping proposals.

### 2.2 The Sensitive-Field Carve-Out & Blast Radius
A fast mistake on an employee's salary or government ID has disastrous compliance ramifications. We evaluate risk based on **blast radius**:
- Low-stakes transformations (whitespace, phone formatting) are automated.
- High-stakes fields require explicit confirmation.

### 2.3 Escalation at the Highest Abstraction Level
Instead of generating 400 escalation cards for 400 rows with an unknown status value `"LOA"`, the agent groups them:
- **One card** asking: *"Map unknown status 'LOA' to 'On Leave' for 40 affected rows?"*
- Selecting **Remember as rule** ensures the agent never asks this question again for this client.

### 2.4 Volume Circuit Breaker
If the open escalation group count exceeds 20, or if more than 30% of total rows are flagged for escalation, the mapping is structurally flawed. The agent trips a circuit breaker and escalates the run itself, preventing reviewer fatigue.

---

## 3. Delta Solutioning (Criterion 6)

What value does this system provide on top of plain AI generation?
1. **Resolution Memory (Rule Synthesis):** Every human review creates a persistent, client-scoped rule that shrinks human intervention on subsequent runs.
2. **Deterministic Safety Guardrails:** Strict schema validation and compensating rollback actions that LLMs cannot provide alone.
3. **Audit Trail & Provenance:** Every record tracks which source file contributed each field, with complete before/after delta logs.
4. **Reconciliation Report & Autonomy Scoreboard:** Gives implementation consultants a verifiable client-facing sign-off document.
