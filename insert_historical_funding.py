import pandas as pd
import mysql.connector
from config import DB_CONFIG_WRITER as writer_config

# === Helper: Clean and validate enum fields ===
def clean_enum(value, allowed_values):
    if pd.isna(value) or str(value).strip() == '':
        return None
    val = str(value).strip().title()
    return val if val in allowed_values else None

# === Load data ===
csv_path = 'data/historical_funding_data.csv'
df = pd.read_csv(csv_path)

# Strip whitespace from headers
df.columns = [col.strip() for col in df.columns]

# Clean up numeric fields with commas
if 'project_direct_costs' in df.columns:
    df['project_direct_costs'] = df['project_direct_costs'].replace({',': ''}, regex=True)
    df['project_direct_costs'] = pd.to_numeric(df['project_direct_costs'], errors='coerce')

# === Connect to MySQL ===
conn = mysql.connector.connect(**writer_config)
cursor = conn.cursor()

# === Insert SQL ===
insert_sql = """
    INSERT INTO funding_sources (
        period_label,
        program_code,
        participant_type,
        source_label,
        category,
        peer_reviewed,
        source_type,
        project_direct_costs,
        total_projects,
        r01_investigators,
        r01_projects
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# === Loop through rows and insert ===
for index, row in df.iterrows():
    try:
        cursor.execute(insert_sql, (
            row['period_label'],
            row['program_code'].strip() if pd.notna(row['program_code']) and row['program_code'].strip() else None,
            row['participant_type'].strip() if pd.notna(row['participant_type']) and row['participant_type'].strip() else None,
            row['source_label'].strip() if pd.notna(row['source_label']) else None,
            clean_enum(row['category'], ['Research', 'Training']),
            clean_enum(row['peer_reviewed'], ['Peer-Reviewed', 'Non-Peer-Reviewed']),
            row['source_type'].strip() if pd.notna(row['source_type']) and row['source_type'].strip() else None,
            float(row['project_direct_costs']) if pd.notna(row['project_direct_costs']) else None,
            int(row['total_projects']) if pd.notna(row['total_projects']) else None,
            int(row['r01_investigators']) if pd.notna(row['r01_investigators']) else None,
            int(row['r01_projects']) if pd.notna(row['r01_projects']) else None,
        ))
    except Exception as e:
        print(f"❌ Error inserting row {index + 1}: {e}")

# === Finalize ===
conn.commit()
cursor.close()
conn.close()

print("✅ Historical funding data successfully inserted into funding_sources table.")
