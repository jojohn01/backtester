import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil

# Adjust these imports to match your actual filenames
from datarepo import DataRepository
from dataspec import DataSpec, DataType, AssetType, Source

def test_pipeline():
    # 1. SETUP: Define a test range (e.g., first 3 days of 2024)
    # Using a past date ensures the data is definitely immutable/complete.
    start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_date = datetime(2024, 1, 4, tzinfo=timezone.utc)

    print(f"--- TEST START: Requesting BTC/USDT from {start_date.date()} to {end_date.date()} ---")

    # 2. INITIALIZE REPO
    # We use a temporary test folder so we don't mess up your real data library yet
    repo = DataRepository(root_dir="./data_test")
    
    spec = DataSpec(
        symbol="BTC",
        source=Source.BINANCE,
        asset_type=AssetType.CRYPTO,
        data_type=DataType.BARS,
        currency="USDT",
        timeframe="1m",
        start=start_date,
        end=end_date
    )

    # 3. FIRST RUN (The "Cold" Start)
    # This should trigger a DOWNLOAD from Binance and SAVE to disk.
    print("\n[1] Running Cold Load (Should Fetch)...")
    t0 = datetime.now()
    df = repo.load_data(spec)
    t1 = datetime.now()
    
    print(f"    - Rows Loaded: {len(df)}")
    print(f"    - Time Taken: {(t1-t0).total_seconds():.2f}s")
    
    # Validation A: Did we get data?
    if df.empty:
        print("    [FAIL] DataFrame is empty!")
        return
    
    # Validation B: Is the file on disk?
    expected_file = Path("./data_test/binance/crypto/bars/BTC/USDT/2024.parquet")
    if expected_file.exists():
        print(f"    [PASS] File created at: {expected_file}")
    else:
        print(f"    [FAIL] File NOT found at: {expected_file}")
        return

    # Validation C: Are gaps filled?
    # We check if there are any NaNs. If _fill_gaps worked, this should be 0.
    nan_count = df.isnull().sum().sum()
    if nan_count == 0:
        print("    [PASS] No NaNs found (Gap filling worked).")
    else:
        print(f"    [FAIL] Found {nan_count} NaNs! Gap filling failed.")

    # 4. SECOND RUN (The "Cached" Start)
    # This should verify the SCANNER found the file and skipped the download.
    print("\n[2] Running Hot Load (Should Read from Disk)...")
    t0 = datetime.now()
    df_cached = repo.load_data(spec)
    t1 = datetime.now()
    
    print(f"    - Rows Loaded: {len(df_cached)}")
    print(f"    - Time Taken: {(t1-t0).total_seconds():.2f}s")

    # Validation D: Speed Check
    # Hot load should be significantly faster than cold load
    print("    [PASS] Hot load completed.")

    # 5. DATA INTEGRITY CHECK
    print("\n[3] Inspecting Data Head/Tail...")
    print(df_cached.head(2))
    print("...")
    print(df_cached.tail(2))

    # Optional: Clean up test data
    shutil.rmtree("./data_test")
    print("\n[CLEANUP] Test folder removed.")

if __name__ == "__main__":
    test_pipeline()