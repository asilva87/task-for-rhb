import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent

SILVER_PATH = BASE_DIR / "data" / "silver" / "trainrides_silver.parquet"
GOLD_DIR = BASE_DIR / "data" / "gold"
GOLD_DIR.mkdir(parents=True, exist_ok=True)

GOLD_PATH = GOLD_DIR / "delay_rate_by_category.parquet"

print("Reading Silver data...")
df = pd.read_parquet(SILVER_PATH)

# Gold aggregation: delay rate by station category
gold = (
    df.groupby("category", as_index=False)
      .agg(
          total_events=("ID", "count"),
          delayed_events=("is_delayed", "sum"),
          delay_rate=("is_delayed", "mean"),
      )
      .sort_values("delay_rate", ascending=False)
)

print("Writing Gold data...")
gold.to_parquet(GOLD_PATH, engine="pyarrow")

print("Done.")
print(gold)
print(f"File written to: {GOLD_PATH}")
