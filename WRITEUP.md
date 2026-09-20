# Darwinbox FDE Take-Home Defense: Autonomous Client Data Migration Agent

**Author:** Anshik Thakur  
**Contact:** +91-7973990693 | thakuranshik5555@gmail.com  
**Project:** DarwinSync AI — Autonomous Client Data Migration & Integration Agent  
**Live Hosted Prototype:** [https://fde-production-dbcc.up.railway.app/](https://fde-production-dbcc.up.railway.app/)  
**Repository:** [github.com/Anshik55/FDE](https://github.com/Anshik55/FDE)  

---

## 1. Guiding Principle: Where the Line is Drawn

> **"The agent automates any transformation that is reversible, verifiable against ground truth, or low-blast-radius; it escalates decisions that are irreversible, high-blast-radius, or genuinely ambiguous."**

In enterprise HR implementations (like Darwinbox), a flawed migration corrupts payroll, hierarchy, and access control. Asking a consultant to hand-review 5,000 rows causes migration fatigue; blindly letting an LLM hallucinate mappings or silent date interpretations causes catastrophic payroll failures. 

This agent draws a mathematically defensible boundary:

```
                  ┌─────────────────────────────────────────────────────────────┐
                  │                 HIGH BLAST-RADIUS / AMBIGUOUS               │
                  │   • Near-duplicates (high name match, same DOB, diff ID)    │
                  │   • Date formats with all days ≤ 12 (DD/MM vs MM/DD)        │
                  │   • Sensitive field mappings (Salary, DOB)                  │
                  │   • Sensitive data conflicts across sources                 │
                  │   • Column mapping ties (top-2 margin < 0.20)               │
                  │   • Push 4xx client errors / Target infrastructure outage   │
                  │                                                             │
                  │              ─── ESCALATE TO HUMAN REVIEW ───               │
                  ├─────────────────────────────────────────────────────────────┤
                  │              ─── AGENT ACTS AUTONOMOUSLY ───                │
                  │                                                             │
                  │   • Whitespace, case normalization, phone E.164             │
                  │   • Column date consensus (any single day > 12 proves format│
                  │   • Exact multi-file duplicates & null gap filling          │
                  │   • Non-sensitive conflicts resolved by source precedence   │
                  │   • Validation fails once → safe auto-fix succeeds          │
                  │   • Push 503 transient failure → exponential retry          │
                  └─────────────────────────────────────────────────────────────┘
```

---

## 2. Escalation Boundary Matrix Summary

| Situation | Agent Action | Escalates? | Rationale |
|---|---|:---:|---|
| **Whitespace / Case / Phone** | Strips, normalizes to E.164 (`+91`) | **No** | Completely reversible; verifiable regex; zero data loss. |
| **Date Format (any day > 12)** | Resolves entire column (e.g. DD/MM/YYYY) | **No** | A single day > 12 is mathematical proof of column format. |
| **Date Format (all days $\le$ 12)** | Holds column format | **Yes** | Genuinely ambiguous (`04/05/2021`). Wrong guess silently shifts hire dates. |
| **Column Mapping (High Margin)** | Maps autonomously ($\ge 0.75$, margin $\ge 0.20$) | **No** | Statistically distinct match across aliases and types. |
| **Ambiguous Header (`ref`)** | Calls LLM for top-2 proposals | **Yes** | Tie between `employee_id` and `manager_id`. Prevents mangling hierarchy. |
| **Sensitive Field Mappings** | Batches into 1 confirmation card | **Yes** | Salary & DOB are load-bearing; one-click approval, remembered forever. |
| **Exact Duplicate Across Files** | Auto-merges and fills missing values | **No** | Corroborating entity identity; non-conflicting values enrich the profile. |
| **Near-Duplicate (Planted Case 4)** | Evaluates fuzzy name $\ge 90$ + identical DOB | **Yes** | Merging two distinct employees into one target ID is irreversible. |
| **Non-Sensitive Conflict** | Resolves via client precedence (`hris > crm > payroll`) | **No** | Logged to reconciliation ledger for optional spot-check. |
| **Sensitive Conflict (Salary)** | Withholds auto-merge for conflicting field | **Yes** | Payroll discrepancy (e.g. 80,000 vs 88,000) requires human sign-off. |
| **Validation: Fails Twice** | Validate $\rightarrow$ Auto-fix $\rightarrow$ Re-validate | **Yes** | If auto-fix fails (e.g. missing required email), escalate immediately. |
| **Push 503 Transient Error** | Retries with exponential backoff & jitter | **No** | Network/service blip; succeeds silently on second attempt. |
| **Push 422 Client Error** | Immediately parks record; no retry | **Yes** | Target API rejects payload as malformed; retrying will never succeed. |
| **Target System Outage** | Halts run, compensating `DELETE` for current batch | **Yes** | Prevents partial dirty writes during downstream system downtime. |

---

## 3. Delta Solutioning: What is Built Beyond Raw AI

The differentiator of an FDE is **not** calling an LLM API; it is the **delta engineering** wrapped around it:

1. **Resolution Memory (Learned Rules):** When a human consultant resolves an escalation (e.g., approving that `"LOA"` maps to `"On Leave"`, or specifying that `ref` means `employee_id`), the decision is fingerprinted and saved as a persistent client-scoped rule (`rules` table). When the pipeline re-runs, the rule fires before the escalation gate. **Escalations drop from ~8 cards on Run 1 down to ~4 cards on Run 2**.
2. **Reconciliation Report & Autonomy Scoreboard:** A client-ready handoff artifact showing rows in $\rightarrow$ merged $\rightarrow$ pushed $\rightarrow$ rejected, dropped column justifications, and source-precedence conflict ledgers. The Autonomy Scoreboard proves to leadership that >90% of data migration labor was automated.
3. **Strict Append-Only Event Spine:** Every action carries `(before, after, reason, score, actor)`. SQLite database triggers prohibit `UPDATE` and `DELETE` on the `events` table, creating an immutable compliance audit trail.
4. **Idempotency & Compensating Rollback:** Every record push generates a SHA256 key from `canonical_key + payload`. Retries never double-create employees. Infrastructure outages trigger an automated compensating rollback (`DELETE /stub/employees/{id}`) to restore target purity.

---

## 4. Empirical Verification (Run 1 vs Run 2)

| Metric | Run 1 (Cold Start) | Run 2 (With Resolution Memory) | Impact / Delta |
|---|:---:|:---:|---|
| **Total Source Rows** | 52 | 52 | Ingested across 3 distinct formats |
| **Canonical Merged Records** | 29 | 29 | Fully deduplicated entities |
| **Escalation Cards Opened** | **7** | **3** | **57% reduction in human review cards** |
| **Status "LOA" Cards** | 1 (grouped for 2 rows) | 0 | Auto-applied learned enum rule |
| **Sensitive Mapping Confirmations**| 1 (batched) | 0 | Auto-applied confirmed mapping rule |
| **Autonomy Score** | **88.2%** | **95.1%** | Visible efficiency gain |

---

## 5. What I Would Build Next in Production

1. **Active Learning Feedback Loop:** Cluster unmapped columns across multiple enterprise clients to suggest industry-standard synonym maps while preserving tenant isolation.
2. **Multi-Entity Relational Graph:** Link employees to Departments, Cost Centers, and Organizational Units with topological dependency ordering during the push stage.
3. **Automated Schema Evolution:** Detect when a client source export format drifts (e.g. new column added or format shifted) and generate an automated diff migration proposal.
