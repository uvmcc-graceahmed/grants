import pandas as pd
import mysql.connector
from mysql.connector import Error
from db_config import writer_config
from datetime import datetime

EXCEL_FILE = "GrantFundingOverTime.xlsx"

# Step 1: Define known reporting periods and their labels
period_labels = ["FY2022", "2022", "FY2023", "2023", "FY2024", "2024", "FY2025"]

# Step 2: Load the Excel file
df = pd.read_excel(EXCEL_FILE, header=None)
period_dates = list(df.iloc[0, 1:])
period_map = {i+1: (label, pd.to_datetime(date).date()) for i, (label, date) in enumerate(zip(period_labels, period_dates))}

# Step 3: Metric mapping logic
program_transitions = {"PSCO": "PSCO", "CHE": "CHE", "CC": "CC", "Education Funding": "Education"}
metric_map = {
    # UVMCC
    "Total Annual Direct Costs - Center": ("funding_summary", "UVMCC", "total_direct_costs"),
    "Total Annual Peer-Reviewed Direct Costs - Center": ("funding_summary", "UVMCC", "peer_reviewed_direct_costs"),
    "Total NCI Annual Direct Costs - Center": ("funding_summary", "UVMCC", "nci_direct_costs"),
    "% NCI Annual Direct Costs - Center": ("funding_summary", "UVMCC", "percent_nci_of_peer_reviewed"),
    "# R01 Investigators": ("funding_summary", "UVMCC", "r01_investigators"),
    "# R01 Awards": ("funding_summary", "UVMCC", "r01_awards"),
    "# Complex Grants": ("funding_summary", "UVMCC", "complex_grants"),
    "% Complex Grants": ("funding_summary", "UVMCC", "percent_complex_grants"),
    "# Multi-Institutional Grants": ("funding_summary", "UVMCC", "multi_institutional_grants"),
    "% Multi-Institutional Grants": ("funding_summary", "UVMCC", "percent_multi_institutional"),
    # Shared PSCO, CHE, CC
    "Total Annual Direct Costs": "total_direct_costs",
    "Total Annual Peer-Reviewed Direct Costs": "peer_reviewed_direct_costs",
    "Total NCI Annual Direct Costs": "nci_direct_costs",
    "% NCI out of Total Peer-Reviewed": "percent_nci_of_peer_reviewed",
    "# R01 Investigators": "r01_investigators",
    "# R01 Awards": "r01_awards",
}
education_fields = {
    "Total Annual Direct Costs": "total_direct_costs",
    "Total Annual Peer-Reviewed Direct Costs": "peer_reviewed_direct_costs",
    "#K Awards": "k_awards",
    "#F Awards": "f_awards"
}

def clean(val):
    if pd.isna(val):
        return None
    if isinstance(val, (float, int)):
        return val
    try:
        return float(str(val).replace(",", ""))
    except:
        return None

def get_or_create_period_id(cursor, period_label, period_end_date):
    cursor.execute("SELECT id FROM reporting_periods WHERE period_label = %s", (period_label,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("INSERT INTO reporting_periods (period_label, period_end_date) VALUES (%s, %s)", (period_label, period_end_date))
    return cursor.lastrowid

def insert_from_excel():
    funding_records = {}
    education_records = {}

    current_program = "UVMCC"
    for row in df.itertuples(index=False):
        key = str(row[0]).strip()
        if key in program_transitions:
            current_program = program_transitions[key]
            continue

        if current_program == "Education":
            field = education_fields.get(key)
            if field:
                for col_idx, val in enumerate(row[1:], start=1):
                    if col_idx in period_map:
                        label, _ = period_map[col_idx]
                        if label not in education_records:
                            education_records[label] = {}
                        education_records[label][field] = val
        elif current_program == "UVMCC":
            mapping = metric_map.get(key)
            if isinstance(mapping, tuple) and len(mapping) == 3:
                table, program, field = mapping
                for col_idx, val in enumerate(row[1:], start=1):
                    if col_idx in period_map:
                        label, _ = period_map[col_idx]
                        funding_records.setdefault((label, program), {})[field] = val
        else:  # PSCO, CHE, CC
            field = metric_map.get(key)
            if isinstance(field, str):
                for col_idx, val in enumerate(row[1:], start=1):
                    if col_idx in period_map:
                        label, _ = period_map[col_idx]
                        funding_records.setdefault((label, current_program), {})[field] = val


    # Step 4: Connect and insert
    try:
        conn = mysql.connector.connect(**writer_config)
        cursor = conn.cursor()

        # Insert funding_summary
        for (label, program), data in funding_records.items():
            period_end = dict(period_map.values())[label]
            period_id = get_or_create_period_id(cursor, label, period_end)
            cursor.execute("""
                INSERT INTO funding_summary (
                    period_id, category,
                    total_direct_costs, peer_reviewed_direct_costs, nci_direct_costs, percent_nci_of_peer_reviewed,
                    r01_investigators, r01_awards,
                    complex_grants, percent_complex_grants,
                    multi_institutional_grants, percent_multi_institutional
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                period_id, program,
                clean(data.get("total_direct_costs")),
                clean(data.get("peer_reviewed_direct_costs")),
                clean(data.get("nci_direct_costs")),
                clean(data.get("percent_nci_of_peer_reviewed")),
                clean(data.get("r01_investigators")),
                clean(data.get("r01_awards")),
                clean(data.get("complex_grants")),
                clean(data.get("percent_complex_grants")),
                clean(data.get("multi_institutional_grants")),
                clean(data.get("percent_multi_institutional"))
            ))

        # Insert education_awards
        for label, data in education_records.items():
            period_end = dict(period_map.values())[label]
            period_id = get_or_create_period_id(cursor, label, period_end)
            cursor.execute("""
                INSERT INTO education_awards (
                    period_id,
                    total_direct_costs, peer_reviewed_direct_costs,
                    k_awards, f_awards,
                    supported_on_t32, supported_on_cobre
                ) VALUES (%s, %s, %s, %s, %s, NULL, NULL)
            """, (
                period_id,
                clean(data.get("total_direct_costs")),
                clean(data.get("peer_reviewed_direct_costs")),
                clean(data.get("k_awards")),
                clean(data.get("f_awards"))
            ))

        conn.commit()
        print("✅ All data successfully inserted.")

    except Error as e:
        print(f"❌ MySQL error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    insert_from_excel()
