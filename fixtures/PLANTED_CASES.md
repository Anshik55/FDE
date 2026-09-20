# Planted Test Cases in Migration Fixtures

The fixtures in this folder (`hris_export.csv`, `payroll_export.xlsx`, `legacy_crm.csv`) contain deliberately planted edge cases to test the agent's autonomy and escalation boundaries deterministically.

## Summary of Planted Cases

| # | Planted Case | Source File(s) | Expected Behavior | Rationale |
|---|---|---|---|---|
| **1** | `ref` column in CRM contains valid unique Employee IDs | `legacy_crm.csv` | **Escalate** (Mapping Ambiguity) | Top candidate scores are close between `employee_id` and `manager_id`. Top-2 margin < 0.20. |
| **2** | All DOJ dates in payroll have day $\le$ 12; HRIS dates have days > 12 | `payroll_export.xlsx` vs `hris_export.csv` | Payroll DOJ **escalates** once; HRIS dates **auto-resolve** to DD/MM/YYYY | Day > 12 proves format. When all days $\le$ 12, DD/MM vs MM/DD is genuinely ambiguous. |
| **3** | Exact duplicate records (E1001-E1025) across HRIS and Payroll | Both files | **Auto-merge** | Same identity key, corroborating information. Non-conflicting values fill gaps. |
| **4** | "Rahul Sharma" vs "Rahul Sherma", identical DOB (1992-05-14), different emails | `hris_export.csv` (`E1012`) vs `legacy_crm.csv` (`E1099`) | **Escalate** (Near-Duplicate) | High name similarity ($\ge 90$) with identical DOB. Risk of merging two separate employees is irreversible. |
| **5** | Salary conflict for E1005 (80,000 in HRIS vs 88,000 in Payroll) | `hris_export.csv` vs `payroll_export.xlsx` | **Escalate** (Sensitive Conflict) | Salary is tagged `sensitive: true`. Discrepancies cannot be silently resolved via precedence. |
| **6** | Status `"LOA"` (2 rows: E1008, E1009) | `hris_export.csv` | **Escalate as 1 Group** (with LLM proposal $\rightarrow$ `"On Leave"`) | Unknown enum value. Grouped into a single card with "Remember as rule" option. |
| **7** | Missing required email for E1030 in all files | `hris_export.csv` | **Escalate** (Fails Validation Twice) | Required field missing, cannot be filled from other files. Auto-fix fails. |
| **8** | Missing email for E1020 in HRIS, but present in CRM | `hris_export.csv` & `legacy_crm.csv` | **Auto-fill** via Reconciliation | Multi-file reconciliation fills missing values from secondary sources. |
| **9** | Messy whitespace, ALL-CAPS names, and mixed phone formats | All files | **Auto-fix** | Completely reversible and verifiable transformations. |
| **10** | Stub API: one 503 retry success, one 422 error | Stub API push | 503 **silently retried**; 422 **escalated** | Transient network issues retry; client errors park the record. |
| **11** | Target API outage (repeated 5xx) | Stub API push | **Compensating Rollback** of current batch | Prevents dirty partial state during infrastructure outages. |
