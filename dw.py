import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

# hardcode the path where your test data lives
folder_path = Path("data_test/binance/crypto/bars/BTC/USDT")
file_path = folder_path / "2024.parquet"

print(f"--- DEBUGGING PYARROW CRASH ---")
print(f"Target File: {file_path}")

if not file_path.exists():
    print("FILE MISSING! Run the test.py first to generate it.")
    exit()

# TEST 1: The "Sanity" Check (Read without filters)
print("\n[TEST 1] Reading file WITHOUT filters (engine='pyarrow')...")
try:
    df_raw = pd.read_parquet(file_path, engine='pyarrow')
    print(f"SUCCESS. Columns: {df_raw.columns.tolist()}")
    print(f"Index Name: {df_raw.index.name}")
    print(f"Rows: {len(df_raw)}")
except Exception as e:
    print(f"FAIL: {e}")

# TEST 2: Check the Schema
print("\n[TEST 2] Inspecting Parquet Schema...")
try:
    schema = pq.read_schema(file_path)
    print(schema)
except Exception as e:
    print(f"FAIL: {e}")

# TEST 3: Reading FILE with filters
# Note: PyArrow handles Timezones strictly. 
# If the file is UTC, we should use a UTC timestamp.
print("\n[TEST 3] Reading FILE with UTC filters...")
try:
    ts_utc = pd.Timestamp("2024-01-01", tz="UTC")
    
    df = pd.read_parquet(
        file_path, 
        engine='pyarrow', 
        filters=[('timestamp', '>=', ts_utc)] 
    )
    print("SUCCESS (File Read).")
except Exception as e:
    print(f"FAIL (File Read): {e}")

# TEST 4: Reading FOLDER with filters
# This is usually where the DLL crash happens on Windows
print("\n[TEST 4] Reading FOLDER with UTC filters...")
try:
    ts_utc = pd.Timestamp("2024-01-01", tz="UTC")
    
    df = pd.read_parquet(
        folder_path, 
        engine='pyarrow', 
        filters=[('timestamp', '>=', ts_utc)]
    )
    print(f"SUCCESS (Folder Read). Loaded {len(df)} rows.")
except Exception as e:
    print(f"FAIL (Folder Read): {e}")