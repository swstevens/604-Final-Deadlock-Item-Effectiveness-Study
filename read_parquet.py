import duckdb
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

con = duckdb.connect()

parquet_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".parquet")]
print(f"Found {len(parquet_files)} parquet files: {parquet_files}\n")

for filename in parquet_files:
    path = os.path.join(DATA_DIR, filename)
    print(f"=== {filename} ===")
    try:
        schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
        print("Columns:")
        for col in schema:
            print(f"  {col[0]}: {col[1]}")
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
        print(f"Rows: {count}")
        df = con.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 5").df()
        print(df.to_string())
    except Exception as e:
        print(f"  ERROR: {e}")
        print("  (File may be incomplete or corrupted — try re-downloading it)")
    print()
