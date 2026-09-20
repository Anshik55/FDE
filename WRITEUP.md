# Darwinbox FDE Take-Home Defense: Autonomous Client Data Migration Agent

**Author:** Forward Deployed Engineer Candidate  
**Project:** DarwinSync AI — Autonomous Client Data Migration & Integration Agent  
**Repository:** [Anshik55/FDE](https://github.com/Anshik55/FDE)  
**Hosted Prototype:** [https://fde-production-dbcc.up.railway.app/](https://fde-production-dbcc.up.railway.app/) *(Synthetic demo data only)*

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
                  │   • Column plausibly maps to 2 fields (top-2 margin < 0.20 )  │
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
                  │   • Column date consensus (any single day > 12 proves format  │
                  │   • Safe validation auto-fix succeeds on first pass           │
                  │   • Exact multi-file duplicates & null gap filling            │
                  │   • Non-sensitive conflicts resolved by source precedence     │
                  │   • Push 503 transient failure → exponential retry            │
                  └───────────────────────────────────────────────────────────────┘
```

---

## 2. Escalation Boundary Matrix (Mirroring Brief Criterion 3)

| Situation | Agent Action | Escalates? | Rationale |
|---|---|:---:|---|
| **Column maps to 2 fields (`ref`)** | Calls LLM for top-2 proposals (`emp_id` vs `mgr_id`) | **Yes** | Ambiguous header (margin < 0.20); prevents mangling organizational hierarchy. |
| **Uncleanable value (Enum `"LOA"`)** | Proposes closest canonical match (`"On Leave"`) | **Yes** | Unrecognized domain enum cannot be safely guessed; persisted as rule once signed off. |
| **Record fails validation twice** | Validates $\rightarrow$ Auto-fix $\rightarrow$ Re-validates | **Yes** | Missing required `email` cannot be synthesized; quarantined to prevent dirty target state. |
| **Push 422 Client Error** | Immediately parks record; halts retry | **Yes** | Target API rejected payload as unprocessable; retrying will never succeed. |
| **Sensitive conflict (Salary)** | Withholds auto-merge for conflicting field | **Yes** | Discrepancy (e.g. ₹80,000 in HRIS vs ₹88,000 in Payroll) requires payroll sign-off. |
| **Ambiguous dates (all days $\le$ 12)** | Holds column format; prompts once per column | **Yes** | Genuinely ambiguous (`04/05/2021`); guessing silently distorts hire dates and tenure. |
| **Near-duplicate identity** | Evaluates fuzzy name $\ge 90$ + identical DOB | **Yes** | Merging two distinct employees into one target ID is an irreversible data loss. |
| **High-margin column mapping** | Maps autonomously ($\ge 0.75$, margin $\ge 0.20$) | **No** | Statistically distinct match across aliases and types; verifiable against target schema. |
| **Cleanable validation format** | Auto-fixes phone to E.164, casing to Title Case | **No** | Completely reversible; verifiable deterministic regex; zero data loss. |
| **Date consensus (any day > 12)** | Resolves entire column (e.g. DD/MM/YYYY) | **No** | A single day > 12 provides mathematical proof of column format across the entire dataset. |
| **Non-sensitive conflict** | Resolves via client precedence (`hris > crm > payroll`) | **No** | Deterministic business hierarchy; logged to reconciliation ledger for audit trail. |
| **Push 503 Transient Error** | Retries with exponential backoff & jitter | **No** | Downstream network/service blip; succeeds silently on subsequent attempt. |

---

## 3. Delta Solutioning: What is Built Beyond Raw AI

The differentiator of an FDE is **not** calling an LLM API; it is the **delta engineering** wrapped around it:

1. **Resolution Memory (Learned Rules):** When a human consultant resolves an escalation (e.g., approving that `"LOA"` maps to `"On Leave"`, or specifying that `ref` means `employee_id`), the decision is fingerprinted and saved as a persistent client-scoped rule (`rules` table). When the pipeline re-runs, the rule fires before the escalation gate. **Review cards drop from 7 on Run 1 down to 3 on Run 2 (-57% reduction) on the planted fixture set.**
2. **Reconciliation Report & Autonomy Scoreboard:** A client-ready handoff artifact showing rows in $\rightarrow$ merged $\rightarrow$ pushed $\rightarrow$ rejected, dropped column justifications, and source-precedence conflict ledgers. Demonstrates efficiency gain grounded in empirical metrics.
3. **Strict Append-Only Event Spine:** Every action carries `(before, after, reason, score, actor)`. SQLite database triggers prohibit `UPDATE` and `DELETE` on the `events` table, creating an immutable compliance audit trail with deterministic replay.
4. **Idempotency & Compensating Rollback:** Every record push generates a SHA256 key from `canonical_key + payload`. Retries never double-create employees. Infrastructure outages trigger an automated compensating rollback (`DELETE /stub/employees/{id}`) to restore target purity.
5. **Enterprise Perimeter Isolation:** The hosted demo uses synthetic fixture data only. In production client deployments, local LLM inference (e.g. Ollama / on-prem vLLM) operates entirely within the enterprise VPC boundary with zero outbound data egress. Cross-tenant synonym matching is strictly opt-in and headers-only.

---

## 4. Empirical Verification (Planted Fixture Set: 52 Source Rows $\rightarrow$ 29 Employees)

*Autonomy Score Definition: `(Automated Decisions / Total Pipeline Actions) × 100%`*

| Metric | Run 1 (Cold Start) | Run 2 (With Resolution Memory) | Impact / Delta (Planted Fixture Set) |
|---|:---:|:---:|---|
| **Total Source Rows** | 52 | 52 | Ingested across 3 distinct formats |
| **Canonical Merged Records** | 29 | 29 | Fully deduplicated entities |
| **Review Cards Opened** | **7** | **3** | **-57% review cards** |
| **Status "LOA" Cards** | 1 (grouped for 2 rows) | 0 | Auto-applied learned enum rule |
| **Sensitive Mapping Confirmations**| 1 (batched) | 0 | Auto-applied confirmed mapping rule |
| **Autonomy Score** | **88.5%** | **95.1%** | Visible efficiency gain |

---

## 5. What I Would Build Next in Production

1. **Active Learning Feedback Loop:** Cluster unmapped column tokens across multiple enterprise clients to suggest industry-standard synonym maps while preserving strict tenant isolation (opt-in and headers-only).
2. **Multi-Entity Relational Graph:** Link employees to Departments, Cost Centers, and Organizational Units with topological dependency ordering during the push stage.
3. **Automated Schema Evolution:** Detect when a client source export format drifts (e.g. new column added or format shifted) and generate an automated diff migration proposal.
