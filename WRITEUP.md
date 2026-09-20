# Darwinbox FDE Take-Home Defense: Autonomous Client Data Migration Agent

**Author:** Forward Deployed Engineer Candidate  
**Project:** DarwinSync AI — Autonomous Client Data Migration & Integration Agent  
**Repository:** [Anshik55/FDE](https://github.com/Anshik55/FDE) (CI Passing &bull; 23 Tests)  
**Hosted Prototype:** [https://fde-production-dbcc.up.railway.app/](https://fde-production-dbcc.up.railway.app/) *(Synthetic demo data only &bull; Reset Demo available)*  
**Attached PDF Deliverable:** `Anshik_Thakur_FDE_Take_Home_Approach.pdf` (1-Page Executive Summary)

---

## 1. Technical Approach & Scoping

**Approach.** I treated this as a scoping problem before a coding one. The agent is a deterministic, event-sourced pipeline (ingest &rarr; profile &rarr; map &rarr; normalize &rarr; reconcile &rarr; validate &rarr; push) where every action is an append-only event carrying before/after, reason, score, and actor. The local LLM is used only where code cannot decide: breaking ties between candidate column mappings and proposing a value for an unrecognized enum. Its suggestions are re-checked against deterministic evidence, and if the model is unavailable the item escalates instead of being guessed.

**How I drew the line.** For every decision I asked three questions: *is it reversible, can code verify it, and what is the blast radius if it is wrong?* Whitespace, phone formats, exact-key duplicates, and a date column where one day exceeds 12 pass all three, so the agent acts. A column that could be employee_id or manager_id, dates where every day is &le; 12, a salary that differs across systems, or two near-identical people with the same DOB fail at least one, so a human decides once and the answer is remembered as a rule until disabled. On the fixtures that took review cards from 7 to 3, and the remaining ones are record-level decisions that cannot be generalized.

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

## 2. Escalation Boundary Defense Matrix (Mirroring Brief Criteria 2 & 3)

| Situation | Agent Action | Escalates? | Rationale & Boundary Defense |
|---|---|:---:|---|
| **Column mapping: ambiguous vs high-margin** | Auto-maps if conf $\ge 0.75$ & margin $\ge 0.20$; calls LLM & prompts if margin $< 0.20$ (`ref`) | **If < 0.20** | High-margin maps autonomously; margin $< 0.20$ (e.g. `emp_id` vs `mgr_id`) destroys reporting hierarchy if guessed. |
| **Sensitive field mapping (Salary / DOB)** | Batches candidate mappings into 1 confirmation card | **Yes** | High blast-radius compliance fields; one-click approval, remembered as rule until disabled. |
| **Uncleanable value (Enum `"LOA"`)** | Proposes closest canonical status (`"On Leave"`) | **Yes** | Unrecognized domain enum cannot be guessed; batched into 1 card, saved as rule until disabled. |
| **Ambiguous dates vs consensus** | Prompts once if all days $\le 12$; auto-resolves if any day $> 12$ | **If all $\le 12$** | `04/05/2021` is ambiguous; guessing shifts hire dates. If any day $> 12$, only one format parses every value in the column. |
| **Near-duplicate identity** | Fuzzy name match $\ge 90$ + identical DOB | **Yes** | Merging two distinct people into one ID is irreversible data loss. Surfaces side-by-side diff. |
| **Sensitive conflict (Salary ₹80k vs ₹88k)** | Withholds auto-merge; surfaces side-by-side diff | **Yes** | Cross-source payroll discrepancy requires human sign-off; non-sensitive conflicts use source precedence. |
| **Record fails validation twice** | Validates $\rightarrow$ Auto-fixes $\rightarrow$ Re-validates failure | **Yes** | Missing required PII (e.g. email) cannot be synthesized; quarantined immediately for review. |
| **Whitespace, casing, phone & exact duplicates** | Strips blanks, title-cases, normalizes E.164 (`+91`); merges exact matches | **No** | Reversible; deterministic regex verification; non-conflicting null fills; zero information loss. |
| **Target API: 503 transient vs 422/400 error** | 503 retries with backoff & jitter; 422/400 parks record & halts retry | **4xx Only** | Network blips succeed on retry; malformed payloads cannot succeed and require human correction/assignment. |

---

## 3. Delta Solutioning: What is Built Beyond Raw AI

The differentiator of an FDE is **not** calling an LLM API; it is the **delta engineering** wrapped around it:

1. **Resolution Memory (Learned Rules Engine):** When a human consultant resolves an escalation (e.g., approving that `"LOA"` maps to `"On Leave"`, or specifying that `ref` means `employee_id`), the decision is fingerprinted and saved as a persistent client-scoped rule (`rules` table). When the pipeline re-runs, the rule fires before the escalation gate. **Pre-push review cards drop from 7 on Run 1 down to 3 on Run 2 (-57% reduction) on the planted fixture set.**
2. **Reconciliation Report & Autonomy Scoreboard:** A client-ready handoff artifact showing rows in $\rightarrow$ merged $\rightarrow$ pushed $\rightarrow$ rejected, dropped column justifications, and source-precedence conflict ledgers. Demonstrates efficiency gain grounded in empirical metrics.
3. **Strict Append-Only Event Spine:** Every action carries `(before, after, reason, score, actor)`. SQLite database triggers prevent `UPDATE` and `DELETE` on the `events` table for auditability.
4. **Target Idempotency & Deterministic Replay:** Target push uses SHA256 `Idempotency-Key` headers to prevent duplicate employee creates. Offline LLM cache uses SHA256 prompt hashing for deterministic replay. Infrastructure outages trigger an automated compensating rollback (`DELETE /stub/employees/{id}`) to restore target purity.
5. **Enterprise Perimeter Isolation:** The hosted demo runs strictly on synthetic test data with an evaluator "Reset Demo" control. In production enterprise deployments, local LLM inference (e.g. Ollama / on-prem vLLM) operates entirely inside the client VPC boundary with **no row-level data leaving the client VPC**. Cross-tenant synonym matching is strictly opt-in and operates only on anonymized header tokens, never row-level PII.

---

## 4. Empirical Verification (Planted Fixture Set: 52 Source Rows $\rightarrow$ 29 Employees)

*Autonomy Score Definition: `(Automated Decisions / Total Pipeline Actions) × 100%` on the planted fixture set.*  
*Counting rule: In Run 2, 3 review cards touch 3 records (salary conflict affects 1 employee, unresolvable validation failure affects 1 employee, near-duplicate card involves 2 candidates counted as 1 distinct unresolved employee slot). 26 of 29 canonical records require zero human touch.*

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

1. **Topological Dependency Graph:** Sequence target entity pushes (Departments $\rightarrow$ Cost Centers $\rightarrow$ Managers $\rightarrow$ Employees) to eliminate referential integrity rejections.
2. **Cross-Tenant Anonymized Embeddings:** Cluster anonymized column header patterns across clients to suggest high-probability synonym maps as pre-warmed recommendations while preserving strict tenant isolation.
3. **Automated Schema Evolution:** Detect when client source exports drift (new columns or shifted formats) and generate automated diff migration proposals.
