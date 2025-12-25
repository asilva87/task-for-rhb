import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent

BRONZE_PATH = BASE_DIR / "data" / "bronze" / "trainrides_bronze.parquet"
SILVER_DIR = BASE_DIR / "data" / "silver"
SILVER_DIR.mkdir(parents=True, exist_ok=True)

SILVER_PATH = SILVER_DIR / "trainrides_silver.parquet"

print("Reading Bronze data...")
df = pd.read_parquet(BRONZE_PATH)

# 1. Type normalization (timestamps)
datetime_cols = [
    "arrival_plan",
    "departure_plan",
    "arrival_change",
    "departure_change",
]

for col in datetime_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

# 2. Basic data-quality rules

# category must be between 1 and 7
df = df[df["category"].between(1, 7)]

# Apply conservative sanity bounds to arrival delays:
# values outside this range are likely data errors (e.g. cancellations or parsing issues),
# while still keeping very large but realistic disruption-related delays.
df = df[df["arrival_delay_m"].between(-60, 300)]

# valid geographic coordinates
df = df[
    df["lat"].between(-90, 90) &
    df["long"].between(-180, 180)
]

# station name must exist
df = df.dropna(subset=["station"])

# remove obvious duplicates
df = df.drop_duplicates(
    subset=["ID", "eva_nr", "arrival_plan"]
)

# 3. Minimal business logic

# Use a >6 minute threshold to flag meaningful operational delays.
# this threshold aligns with the dataset's own delay indicator.
df["is_delayed"] = df["arrival_delay_m"] > 6

# 4. Write Silver layer
print("Writing Silver data...")
df.to_parquet(SILVER_PATH, engine="pyarrow")

print("Done.")
print(f"Rows in Silver: {len(df):,}")
print(f"File written to: {SILVER_PATH}")
