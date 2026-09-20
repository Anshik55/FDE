"""Phase 0 verification tests: schema loading and fixture validation."""

from pathlib import Path
import pandas as pd
import pytest

from app.pipeline.schema_loader import TargetSchema

BASE_DIR = Path(__file__).resolve().parent.parent


def test_schema_loading():
    schema_path = BASE_DIR / "schema" / "target_employee.yaml"
    schema = TargetSchema.from_yaml(schema_path)

    assert schema.entity == "employee"
    assert "employee_id" in schema.fields
    assert "salary" in schema.fields

    # Check identity fields
    identity_fields = [f.name for f in schema.get_identity_fields()]
    assert "employee_id" in identity_fields
    assert "email" in identity_fields

    # Check sensitive fields
    sensitive_fields = [f.name for f in schema.get_sensitive_fields()]
    assert "salary" in sensitive_fields
    assert "date_of_birth" in sensitive_fields

    # Check required fields
    required_fields = [f.name for f in schema.get_required_fields()]
    assert "hire_date" in required_fields
    assert "department" in required_fields


def test_fixtures_load_and_contain_planted_cases():
    hris_path = BASE_DIR / "fixtures" / "hris_export.csv"
    payroll_path = BASE_DIR / "fixtures" / "payroll_export.xlsx"
    crm_path = BASE_DIR / "fixtures" / "legacy_crm.csv"

    assert hris_path.exists()
    assert payroll_path.exists()
    assert crm_path.exists()

    df_hris = pd.read_csv(hris_path)
    df_payroll = pd.read_excel(payroll_path)
    df_crm = pd.read_csv(crm_path)

    assert len(df_hris) == 35
    assert len(df_payroll) == 25
    assert len(df_crm) == 22

    # Verify Case 6: "LOA" in HRIS
    loa_rows = df_hris[df_hris["Status"] == "LOA"]
    assert len(loa_rows) == 2

    # Verify Case 5: Conflicting salary for E1005
    hris_sal = df_hris[df_hris["Emp No"] == "E1005"]["Salary"].iloc[0]
    payroll_sal = df_payroll[df_payroll["EmpID"] == "E1005"]["Gross Salary"].iloc[0]
    assert hris_sal == 80000
    assert payroll_sal == 88000
    assert hris_sal != payroll_sal

    # Verify Case 2: Payroll DOJ has all days <= 12
    doj_values = df_payroll["DOJ"].dropna().tolist()
    for d in doj_values:
        day = int(str(d).split("/")[0])
        assert day <= 12, f"DOJ day {day} should be <= 12 for ambiguity"

    # Verify Case 4: Near duplicate in CRM
    assert "Rahul Sherma" in df_crm["name"].values
    assert "Rahul Sharma" in df_hris["First Name"].str.cat(df_hris["Last Name"], sep=" ").values

    # Verify Case 1: 'ref' column exists in CRM
    assert "ref" in df_crm.columns
