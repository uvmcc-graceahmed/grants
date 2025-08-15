import os
import pandas as pd

# Directory where your Excel files are stored
DATA_DIR = "data"
OUTPUT_CSV = "funding_sources_import.csv"

# Maps used for inferring values
SOURCE_TYPE_MAP = {
    "NCI": "NCI",
    "NIH": "Other NIH",
    "Industry": "Industry"
}

def infer_source_type(source_label):
    for key, val in SOURCE_TYPE_MAP.items():
        if key in source_label:
            return val
    return "Other"

def infer_category(source_label):
    return "Training" if "Training" in source_label else "Research"

def infer_peer_reviewed(source_label):
    if "Peer-Reviewed" in source_label:
        return "Peer-Reviewed"
    elif "Non-Peer-Reviewed" in source_label:
        return "Non-Peer-Reviewed"
    return None

def process_excel_file(filepath):
    print(f"📄 Reading {filepath}...")
    df = pd.read_excel(filepath, header=None)

    # Infer period label from filename (e.g., DT2A_FY2023.xlsx → FY2023)
    filename = os.path.basename(filepath)
    period_label = filename.replace("DT2A_", "").replace(".xlsx", "").upper()

    # Identify rows with valid funding data (usually start after "Specific Funding Source")
    start_row = df[df.iloc[:, 0].astype(str).str.contains("Specific Funding Source", na=False)].index
    if len(start_row) == 0:
        print(f"⚠️ Could not find header row in {filename}")
        return pd.DataFrame()

    df = df.iloc[start_row[0]+1:].copy()
    df.columns = ["participant_type", "source_label", "project_direct_costs", "total_projects", "r01_investigators", "r01_projects"]
    df = df.reset_index(drop=True)

    # Forward-fill participant type (e.g., 'UVM Based', 'Collaborating Members')
    df["participant_type"] = df["participant_type"].fillna(method="ffill")

    # Drop subtotal/summary rows
    df = df[~df["source_label"].astype(str).str.lower().str.contains("subtotal|total", na=False)].copy()

    # Infer additional columns
    df["period_label"] = period_label
    df["program_code"] = "UVMCC"  # You can enhance this logic later if needed
    df["category"] = df["source_label"].apply(infer_category)
    df["peer_reviewed"] = df["source_label"].apply(infer_peer_reviewed)
    df["source_type"] = df["source_label"].apply(infer_source_type)

    # Reorder and ensure correct types
    final_cols = [
        "period_label", "program_code", "participant_type", "source_label",
        "category", "peer_reviewed", "source_type",
        "project_direct_costs", "total_projects", "r01_investigators", "r01_projects"
    ]
    df = df[final_cols]

    return df

def main():
    all_data = []
    for fname in os.listdir(DATA_DIR):
        if fname.endswith(".xlsx"):
            fpath = os.path.join(DATA_DIR, fname)
            df = process_excel_file(fpath)
            if not df.empty:
                all_data.append(df)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.to_csv(OUTPUT_CSV, index=False)
        print(f"✅ Exported {len(combined_df)} rows to {OUTPUT_CSV}")
    else:
        print("❌ No data extracted.")

if __name__ == "__main__":
    main()
