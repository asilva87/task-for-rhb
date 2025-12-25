import pandas as pd
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent
CSV_PATH = BASE_DIR / "data" / "DBtrainrides.csv"
BRONZE_DIR = BASE_DIR / "data" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

PARQUET_PATH = BRONZE_DIR / "trainrides_bronze.parquet"

print("Reading CSV...")
df = pd.read_csv(CSV_PATH)

print("Writing Parquet...")
df.to_parquet(PARQUET_PATH, engine="pyarrow")

print("Done.")
print(f"Rows: {len(df):,}")
print(f"File written to: {PARQUET_PATH}")
