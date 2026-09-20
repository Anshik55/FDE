"""Generate deterministic, seeded fixtures for data migration agent.

Generates:
1. fixtures/hris_export.csv
2. fixtures/payroll_export.xlsx
3. fixtures/legacy_crm.csv
4. fixtures/PLANTED_CASES.md
"""

from pathlib import Path
import random
import pandas as pd

random.seed(42)

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"
FIXTURES_DIR.mkdir(parents=True, exist_ok=True)


def generate_fixtures():
    # 1. Base Employee Catalog
    first_names = [
        "Aarav", "Aditi", "Amit", "Ananya", "Arjun", "Deepak", "Divya", "Gaurav",
        "Ishaan", "Kavita", "Manish", "Meera", "Neha", "Nikhil", "Pooja", "Pranav",
        "Priya", "Rajesh", "Ritu", "Rohan", "Sanjay", "Shreya", "Siddharth", "Sneha",
        "Sunil", "Tanvi", "Varun", "Vikram", "Vivek", "Zoya", "Kunal", "Preeti",
        "Sameer", "Swati", "Tarun"
    ]
    last_names = [
        "Sharma", "Patel", "Verma", "Rao", "Gupta", "Mehta", "Singh", "Kumar",
        "Nair", "Joshi", "Chopra", "Deshmukh", "Reddy", "Bose", "Kulkarni", "Mishra",
        "Saxena", "Pandey", "Iyer", "Chauhan"
    ]

    departments = ["Engineering", "HR", "Finance", "Sales", "Operations"]
    titles = [
        "Software Engineer", "Senior Developer", "Product Manager", "HR Specialist",
        "Financial Analyst", "Account Executive", "Operations Lead", "QA Analyst"
    ]

    # -------------------------------------------------------------
    # Generate 35 core employees
    # -------------------------------------------------------------
    employees = []
    for i in range(1, 36):
        emp_id = f"E{1000 + i}"
        fn = first_names[(i - 1) % len(first_names)]
        ln = last_names[(i - 1) % len(last_names)]
        birth_day = 13 + ((i * 3) % 15)  # 13 to 27
        birth_month = 1 + (i % 12)
        birth_year = 1985 + (i % 12)
        if emp_id == "E1012":
            fn = "Rahul"
            ln = "Sharma"
            dob = "14/05/1992"
        elif emp_id == "E1020":
            fn = "Vikram"
            ln = "Mehta"
            dob = f"{birth_day:02d}/{birth_month:02d}/{birth_year}"
        else:
            dob = f"{birth_day:02d}/{birth_month:02d}/{birth_year}"
        email = f"{fn.lower()}.{ln.lower()}@example.com"

        join_day = 13 + ((i * 2) % 15)  # 13 to 27
        join_month = 1 + ((i + 3) % 12)
        join_year = 2020 + (i % 4)
        join_date = f"{join_day:02d}/{join_month:02d}/{join_year}"

        dept = departments[i % len(departments)]
        title = titles[i % len(titles)]
        mgr = f"E{1000 + ((i % 5) + 1)}" if i > 5 else ""
        salary = 60000 + (i * 2500)
        status = "Active"

        employees.append({
            "id": emp_id,
            "first_name": fn,
            "last_name": ln,
            "email": email,
            "dob": dob,
            "join_date": join_date,
            "dept": dept,
            "title": title,
            "mgr": mgr,
            "salary": salary,
            "status": status,
        })

    # -------------------------------------------------------------
    # Plant cases in HRIS
    # -------------------------------------------------------------
    hris_rows = []
    for e in employees:
        row = {
            "Emp No": e["id"],
            "First Name": e["first_name"],
            "Last Name": e["last_name"],
            "E-mail": e["email"],
            "DOB": e["dob"],
            "Join Date": e["join_date"],
            "Dept": e["dept"],
            "Designation": e["title"],
            "Status": e["status"],
            "Mgr": e["mgr"],
            "Salary": e["salary"],
        }
        # Casing/whitespace noise
        if e["id"] in ["E1002", "E1007"]:
            row["First Name"] = f"  {e['first_name'].upper()}  "
            row["Last Name"] = f" {e['last_name'].lower()} "
            row["E-mail"] = f"  {e['email']} "
            row["Designation"] = f"  {e['title'].lower()}  "

        # Planted Case 5: Sensitive Salary Conflict (HRIS has 80000, Payroll will have 88000)
        if e["id"] == "E1005":
            row["Salary"] = 80000

        # Planted Case 6: "LOA" unknown enum (2 rows)
        if e["id"] in ["E1008", "E1009"]:
            row["Status"] = "LOA"

        # Planted Case 8: Missing email in HRIS (Vikram Mehta), present in CRM
        if e["id"] == "E1020":
            row["E-mail"] = ""

        # Planted Case 7: Completely missing email across all files
        if e["id"] == "E1030":
            row["E-mail"] = ""

        hris_rows.append(row)

    df_hris = pd.DataFrame(hris_rows)
    hris_csv_path = FIXTURES_DIR / "hris_export.csv"
    df_hris.to_csv(hris_csv_path, index=False)
    print(f"Created {hris_csv_path} ({len(df_hris)} rows)")

    # -------------------------------------------------------------
    # Plant cases in Payroll Export (Excel)
    # Features:
    # - Headers: EmpID, Full Name, Email Address, Gross Salary, DOJ
    # - DOJ dates: ALL values have day <= 12 -> Ambiguous date format!
    # -------------------------------------------------------------
    payroll_rows = []
    for i, e in enumerate(employees[:25]):
        # Deliberately construct DOJ where BOTH day and month are <= 12
        # e.g., 04/05/2021 (could be April 5 or May 4)
        ambiguous_day = 1 + (i % 12)
        ambiguous_month = 1 + ((i * 2) % 12)
        ambiguous_doj = f"{ambiguous_day:02d}/{ambiguous_month:02d}/{2020 + (i % 4)}"

        salary_val = e["salary"]
        if e["id"] == "E1005":
            salary_val = 88000  # Conflicting salary with HRIS!

        email_val = e["email"]
        if e["id"] == "E1020":
            email_val = ""  # Missing in Payroll too, so CRM fills it!

        payroll_rows.append({
            "EmpID": e["id"],
            "Full Name": f"{e['first_name']} {e['last_name']}",
            "Email Address": email_val,
            "Gross Salary": salary_val,
            "DOJ": ambiguous_doj,
        })

    df_payroll = pd.DataFrame(payroll_rows)
    payroll_xlsx_path = FIXTURES_DIR / "payroll_export.xlsx"
    df_payroll.to_excel(payroll_xlsx_path, index=False)
    print(f"Created {payroll_xlsx_path} ({len(df_payroll)} rows)")

    # -------------------------------------------------------------
    # Plant cases in Legacy CRM (CSV)
    # Features:
    # - Headers: name, mail, phone, birth_date, ref, status, dept
    # - Column `ref`: all unique valid employee IDs -> ambiguous mapping (employee_id vs manager_id)
    # - Mixed date formats in birth_date
    # - Phone noise (spaces, dashes, country code)
    # - Near-duplicate: "Rahul Sherma" (Case 4)
    # -------------------------------------------------------------
    crm_rows = []
    # Subset of employees from 15 to 35
    for i, e in enumerate(employees[14:], start=15):
        # birth_date mixed: ISO or '15-Mar-1988'
        if i % 2 == 0:
            b_date = f"{1985 + (i % 12)}-{1 + (i % 12):02d}-15"
        else:
            b_date = f"15-May-{1985 + (i % 12)}"

        # Phone noise
        phone_num = f"+91 {9876000000 + i}" if i % 2 == 0 else f"09876{500000 + i}"

        crm_email = e["email"]
        if e["id"] == "E1020":
            crm_email = "vikram.mehta@example.com"
        elif e["id"] == "E1030":
            crm_email = ""  # Case 7: Completely missing email across all files

        crm_rows.append({
            "name": f"{e['first_name']} {e['last_name']}",
            "mail": crm_email,
            "phone": phone_num,
            "birth_date": e["dob"],
            "ref": e["id"],  # Valid employee ID, but header says 'ref'
            "status": e["status"].lower(),
            "dept": e["dept"].lower(),
        })

    # Planted Case 4: Near-duplicate "Rahul Sherma"
    # Same birth_date as Rahul Sharma (1992-05-14), slightly altered name and new email/ref
    crm_rows.append({
        "name": "Rahul Sherma",
        "mail": "rahul.sherma@consultant-network.org",
        "phone": "+91 9988776655",
        "birth_date": "1992-05-14",
        "ref": "E1099",
        "status": "active",
        "dept": "tech",
    })

    df_crm = pd.DataFrame(crm_rows)
    crm_csv_path = FIXTURES_DIR / "legacy_crm.csv"
    df_crm.to_csv(crm_csv_path, index=False)
    print(f"Created {crm_csv_path} ({len(df_crm)} rows)")

    # -------------------------------------------------------------
    # Generate PLANTED_CASES.md documentation
    # -------------------------------------------------------------
    planted_cases_content = r"""# Planted Test Cases in Migration Fixtures

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
"""
    planted_cases_path = FIXTURES_DIR / "PLANTED_CASES.md"
    with open(planted_cases_path, "w", encoding="utf-8") as f:
        f.write(planted_cases_content)
    print(f"Created {planted_cases_path}")


if __name__ == "__main__":
    generate_fixtures()
