import pandas as pd
import sys

INPUT_FILE = "measurements-out.csv"
OUTPUT_FILE = "measurements-cleaned.csv"
LIMIT = 2000000
TIMESTAMP_COL = "Captured Time"


df = pd.read_csv(
  INPUT_FILE,
  parse_dates=[TIMESTAMP_COL],
  low_memory=False
)


# Filter out future or invalid timestamps
df[TIMESTAMP_COL] = pd.to_datetime(df[TIMESTAMP_COL], utc=True)
now = pd.Timestamp.now(tz="UTC")
df = df[df[TIMESTAMP_COL] <= now]


# Sort ascending by timestamp
df = df.sort_values(TIMESTAMP_COL)


# Validation
null_count = df[TIMESTAMP_COL].isna().sum()
if null_count > 0:
  print(f"Error: Found {null_count} missing timestamps in column '{TIMESTAMP_COL}'!")
  sys.exit(1)

if not df[TIMESTAMP_COL].is_monotonic_increasing:
  print(f"Error: Timestamps in '{TIMESTAMP_COL}' are not fully sorted ascending!")
  sys.exit(1)


# Truncate if above limit
total = len(df)
if total > LIMIT:
  df = df.iloc[:LIMIT]


# Write out cleaned data
df.to_csv(OUTPUT_FILE, index=False)