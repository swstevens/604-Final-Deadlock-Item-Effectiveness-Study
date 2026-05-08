import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json

# ── INPUT ────────────────────────────────────────────────────────────────────
INPUT_JSON  = "data.json"   # replace with actual path to your JSON file
OUTPUT_FILE = "output.parquet"

# ── LOAD ─────────────────────────────────────────────────────────────────────
with open(INPUT_JSON, "r") as f:
    raw = json.load(f)

# If your JSON is a list of records, this works directly.
# If it's nested (e.g. {"data": [...]}), index into it:
#   raw = raw["data"]  # replace "data" with actual top-level key
df = pd.DataFrame(raw)

# ── COLUMN SELECTION ─────────────────────────────────────────────────────────
# Replace the values below with the actual column names from your JSON.
# Remove any columns not needed in the final parquet.
df = df[[
    "COLUMN_CHARACTER",     # character name or ID
    "COLUMN_ARCHETYPE",     # character archetype/class
    "COLUMN_ITEM",          # item name or ID
    "COLUMN_ITEM_CATEGORY", # item category/type
    "COLUMN_OUTCOME",       # win/loss — should be binary (1/0 or True/False)
    # add more columns as needed
]]

# ── RENAME (optional) ────────────────────────────────────────────────────────
# Standardize column names for downstream analysis if needed.
df = df.rename(columns={
    "COLUMN_CHARACTER":     "character",
    "COLUMN_ARCHETYPE":     "archetype",
    "COLUMN_ITEM":          "item",
    "COLUMN_ITEM_CATEGORY": "item_category",
    "COLUMN_OUTCOME":       "win",
})

# ── TYPE CASTING ─────────────────────────────────────────────────────────────
# Ensure the outcome column is integer (1/0) for logistic regression.
df["win"] = df["win"].astype(int)

# Cast categorical columns explicitly if desired (reduces parquet file size).
# df["archetype"]     = df["archetype"].astype("category")
# df["item_category"] = df["item_category"].astype("category")

# ── SCHEMA (optional but recommended) ────────────────────────────────────────
# Define an explicit Arrow schema to enforce types in the parquet file.
# Adjust pa types to match your actual data.
schema = pa.schema([
    pa.field("character",     pa.string()),
    pa.field("archetype",     pa.string()),
    pa.field("item",          pa.string()),
    pa.field("item_category", pa.string()),
    pa.field("win",           pa.int8()),   # binary outcome
    # add more fields as needed
])

# ── WRITE ─────────────────────────────────────────────────────────────────────
table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
pq.write_table(table, OUTPUT_FILE, compression="snappy")

print(f"Wrote {len(df)} rows to {OUTPUT_FILE}")
