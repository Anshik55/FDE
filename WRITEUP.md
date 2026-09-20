# Darwinbox FDE Take-Home Defense: Autonomous Client Data Migration Agent

**Author:** Forward Deployed Engineer Candidate  
**Project:** DarwinSync AI — Autonomous Client Data Migration & Integration Agent  
**Repository:** [Anshik55/FDE](https://github.com/Anshik55/FDE)  
**Hosted Prototype:** [https://fde-production-dbcc.up.railway.app/](https://fde-production-dbcc.up.railway.app/) *(Synthetic demo data only &bull; Reset Demo available)*

---

## 1. Guiding Principle: Where the Line is Drawn

> **"AI proposes, code verifies, and humans sign off where errors are irreversible or ambiguous."**

In enterprise HR implementations (like Darwinbox), a flawed migration corrupts payroll, hierarchy, and access control. Asking a consultant to hand-review 5,000 rows causes migration fatigue; blindly letting an LLM hallucinate mappings or silent date interpretations causes catastrophic payroll failures. 

This agent draws a mathematically defensible boundary:

```
                  ┌───────────────────────────────────────────────────────────────┐
                  │                 HIGH BLAST-RADIUS / AMBIGUOUS                 │
                  │   • Near-duplicates (high name match, same DOB, diff ID)      │ 
                  │   • Date formats with all days ≤ 12 (DD/MM vs MM/DD)          │
                  │   • Column plausibly maps to 2 fields (top-2 margin < 0.20)   │
                  │   • Value agent cannot clean (unknown enum e.g. "LOA")        │
                  │   • Record fails validation twice (unresolvable missing PII)  │
                  │   • Sensitive field conflicts across sources (Salary)         │
                  │   • Push 422 Client Error / Target infrastructure outage      │
                  │                                                               │
                  │              ─── ESCALATE TO HUMAN REVIEW ───                 │
                  ├───────────────────────────────────────────────────────────────┤
                  │              ─── AGENT ACTS AUTONOMOUSLY ───                  │
                  │                                                               │
                  │   • High-confidence column mapping (margin ≥ 0.20, conf ≥0.75)│
                  │   • Column date consensus (any single day > 12 proves format) │
                  │   • Safe validation auto-fix succeeds on first pass           │
                  │   • Exact multi-file duplicates & null gap filling            │
                  │   • Non-sensitive conflicts resolved by source precedence     │
                  │   • Push 503 transient failure → exponential retry            │
                  └───────────────────────────────────────────────────────────────┘
```

---

## 2. Escalation Boundary Matrix (Mirroring Brief Criteria 2 & 3)

| Situation | Agent Action | Escalates? | Rationale & Boundary Defense |
|---|---|:---:|---|
| **Column maps to 2 fields (`ref`)** | Calls LLM for top-2 proposals (`emp_id` vs `mgr_id`) | **Yes** | Margin < 0.20; guessing destroys reporting hierarchy. Prompts consultant once. |
| **High-margin column mapping** | Maps autonomously (conf $\ge 0.75$, margin $\ge 0.20$) | **No** | Statistically distinct match across aliases and types; verifiable against target schema. |
| **Sensitive field mapping (Salary/DOB)** | Batches candidate mappings into 1 confirmation card | **Yes** | High blast-radius compliance fields; one-click approval, remembered forever as rule. |
| **Value cannot be cleaned (`"LOA"`)** | Proposes closest canonical status (`"On Leave"`) | **Yes** | Unrecognized domain enum cannot be guessed; batched into 1 card, saved as rule. |
| **Ambiguous dates (all days $\le$ 12)** | Holds format; prompts once per column (DD/MM vs MM/DD) | **Yes** | `04/05/2021` is genuinely ambiguous. Blind guesses silently shift hire dates. |
| **Date consensus (any single day > 12)** | Resolves entire column format (e.g. DD/MM/YYYY) | **No** | A single day > 12 provides mathematical proof of column format across dataset. |
| **Near-duplicate identity** | Fuzzy name match $\ge 90$ + identical DOB | **Yes** | Merging two distinct people into one ID is irreversible data loss. |
| **Obvious duplicate across files** | Exact match on candidate key; merges and fills nulls | **No** | Corroborating record identity enriches profile; non-conflicting null fills. |
| **Sensitive conflict (Salary)** | Withholds auto-merge; surfaces side-by-side diff | **Yes** | Discrepancy (₹80,000 in HRIS vs ₹88,000 in Payroll); requires human sign-off. |
| **Non-sensitive conflict** | Resolves via client precedence (`hris > crm > payroll`) | **No** | Deterministic business hierarchy; logged to reconciliation ledger for audit trail. |
| **Record fails validation twice** | Validates $\rightarrow$ Auto-fixes $\rightarrow$ Re-validates failure | **Yes** | Missing required PII (e.g. email) cannot be synthesized; quarantined immediately. |
| **Whitespace, casing & phone** | Strips blanks, Capitalizes Name, normalizes E.164 (`+91`) | **No** | Reversible; deterministic regex verification; zero information loss. |
| **Target Push 422 Client Error** | Halts retry; parks record; logs target 4xx rejection | **Yes** | Target rejects malformed payload; retrying is futile. Pre-push review cards exclude 422. |
| **Target Push 503 Transient Error** | Exponential backoff with jitter (max 3 retries) | **No** | Downstream network/service blip; succeeds silently on subsequent attempt. |

---

## 3. Delta Solutioning: What is Built Beyond Raw AI

The differentiator of an FDE is **not** calling an LLM API; it is the **delta engineering** wrapped around it:

1. **Resolution Memory (Learned Rules Engine):** When a human consultant resolves an escalation (e.g., approving that `"LOA"` maps to `"On Leave"`, or specifying that `ref` means `employee_id`), the decision is fingerprinted and saved as a persistent client-scoped rule (`rules` table). When the pipeline re-runs, the rule fires before the escalation gate. **Pre-push review cards drop from 7 on Run 1 down to 3 on Run 2 (-57% reduction) on the planted fixture set.**
2. **Reconciliation Report & Autonomy Scoreboard:** A client-ready handoff artifact showing rows in $\rightarrow$ merged $\rightarrow$ pushed $\rightarrow$ rejected, dropped column justifications, and source-precedence conflict ledgers. Demonstrates efficiency gain grounded in empirical metrics.
3. **Strict Append-Only Event Spine:** Every action carries `(before, after, reason, score, actor)`. SQLite database triggers prohibit `UPDATE` and `DELETE` on the `events` table, creating an immutable compliance audit log.
4. **Target Idempotency & Deterministic Replay:** Target push uses SHA256 `Idempotency-Key` headers to prevent duplicate employee creates. Offline LLM cache uses SHA256 prompt hashing for deterministic replay. Infrastructure outages trigger an automated compensating rollback (`DELETE /stub/employees/{id}`) to restore target purity.
5. **Enterprise Perimeter Isolation:** The hosted demo runs strictly on synthetic test data with an evaluator "Reset Demo" control. In production enterprise deployments, local LLM inference (e.g. Ollama / on-prem vLLM) operates entirely inside the client VPC boundary with **no row-level data leaving the client VPC**. Cross-tenant synonym matching is strictly opt-in and operates only on anonymized header tokens, never row-level PII.

---

## 4. Empirical Verification (Planted Fixture Set: 52 Source Rows $\rightarrow$ 29 Employees)

*Autonomy Score Definition: `(Automated Decisions / Total Pipeline Actions) × 100%` on the planted fixture set. On Run 2, 26 of 29 (90%) canonical employees required zero human intervention.*

| Metric | Run 1 (Cold Start) | Run 2 (With Resolution Memory) | Impact / Delta (Planted Fixture Set) |
|---|:---:|:---:|---|
| **Total Source Rows** | 52 | 52 | Ingested across 3 distinct formats |
| **Canonical Merged Records** | 29 | 29 | Fully deduplicated entities |
| **Pre-Push Review Cards Opened** | **7** | **3** | **-57% review cards** |
| **Status "LOA" Cards** | 1 (grouped for 2 rows) | 0 | Auto-applied learned enum rule |
| **Sensitive Mapping Confirmations**| 1 (batched) | 0 | Auto-applied confirmed mapping rule |
| **Canonical Employees Zero-Touch** | 22 of 29 (76%) | 26 of 29 (90%) | +14% increase in untouched entities |
| **Action Autonomy Score** | **88.5%** | **95.1%** | Visible efficiency gain |

---

## 5. What I Would Build Next in Production

1. **Active Learning Feedback Loop:** Cluster unmapped column tokens across multiple enterprise clients to suggest industry-standard synonym maps while preserving strict tenant isolation (opt-in and headers-only).
2. **Multi-Entity Relational Graph:** Link employees to Departments, Cost Centers, and Organizational Units with topological dependency ordering during the push stage.
3. **Automated Schema Evolution:** Detect when a client source export format drifts (e.g. new column added or format shifted) and generate an automated diff migration proposal.
