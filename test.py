import shutil
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from datarepo import DataRepository
# Adjust this import based on where your DataSpec class is located
from dataspec import DataSpec, Source, AssetType, DataType
from typing import cast

# --- SETUP ---
TEST_DIR = Path("data_test")
# Clean start
if TEST_DIR.exists(): shutil.rmtree(TEST_DIR)

repo = DataRepository(root_dir=str(TEST_DIR))

# 1. DEFINE PROXY (The Real Data Source)
# This is the "Raw" data we will actually fetch (Binance BTC)
btc_spec = DataSpec(
    symbol="BTC",
    currency="USDT",
    source=Source.BINANCE,
    asset_type=AssetType.CRYPTO,
    data_type=DataType.BARS,
    timeframe="1m",
    start=datetime(2024, 1, 1, tzinfo=timezone.utc) # Placeholder start
)

# 2. DEFINE STRATEGY ASSET (The Wrapper)
# We pretend this is "SPY" stock data, but we force it to read the BTC proxy.
# We also apply strict NYSE Trading Hours + 30min pre-market padding.
spy_spec = DataSpec(
    symbol="FAKE_SPY", 
    currency="USD",
    source=Source.BINANCE, # This gets ignored because proxy is active
    asset_type=AssetType.EQUITY,
    data_type=DataType.BARS,
    timeframe="1m",
    
    # Request Jan 3rd to Jan 5th
    start=datetime(2024, 1, 3, tzinfo=timezone.utc),
    end=datetime(2024, 1, 5, tzinfo=timezone.utc),
    
    # --- PROXY CONFIG ---
    proxy=btc_spec,
    proxy_tag=True,   # We set this to True to enable the proxy logic
    
    # --- RTH CONFIG ---
    calendar="NYSE",       # Use NYSE calendar (9:30 - 16:00 ET)
    use_rth=True,          # Turn on the filter
    rth_pad_open=30,       # Start session 30 mins early (9:00 AM ET)
    rth_pad_close=0        # Close exactly at 16:00 ET
)

print("\n--- TEST START: Proxy (BTC) -> Asset (SPY) + RTH + Padding ---")
print(f"Requesting: {spy_spec.symbol} (using {btc_spec.symbol} data)")
print(f"Window: {spy_spec.start} to {spy_spec.end}")
print(f"RTH: NYSE (09:30-16:00) with 30m Open Padding (Start 09:00)")

# --- EXECUTE ---
try:
    df = repo.load_data(spy_spec)
except Exception as e:
    print(f"\n[!!!] CRASH: {e}")
    import traceback
    traceback.print_exc()
    df = pd.DataFrame()

# --- VERIFY ---
print(f"\n[RESULT] Rows Loaded: {len(df)}")

if df.empty:
    print("    [FAIL] No data returned.")
else:
    # 1. Verify File System (Did we save into the PROXY folder?)
    # Path should be: data_test/binance/crypto/bars/BTC/USDT (Not FAKE_SPY)
    expected_path = TEST_DIR / "binance" / "crypto" / "bars" / "BTC" / "USDT"
    
    if expected_path.exists() and any(expected_path.iterdir()):
        print("    [PASS] Data correctly stored in PROXY folder (BTC/USDT).")
    else:
        print(f"    [FAIL] Data not found in {expected_path}")

    # 2. Verify RTH Filtering (Did we cut out the nights?)
    # Convert index to Eastern Time to check hours easily
    df_et = cast(pd.DatetimeIndex, df.index).tz_convert('US/Eastern')
    
    earliest_time = df_et.time.min()
    latest_time = df_et.time.max()
    
    print(f"    - Earliest Bar (ET): {earliest_time}")
    print(f"    - Latest Bar (ET):   {latest_time}")

    # Check Start Time (Should be 09:00:00 because of 30m padding)
    if earliest_time.hour == 9 and earliest_time.minute == 0:
        print("    [PASS] RTH Start correct (09:00 ET).")
    elif earliest_time.hour == 9 and earliest_time.minute == 30:
        print("    [FAIL] Padding ignored (Started at 09:30 ET).")
    else:
        print(f"    [WARN] Unexpected start time: {earliest_time}")

    # Check for Overnight Data (Should be zero)
    # Valid hours are 09, 10, 11, 12, 13, 14, 15
    # 16:00 is usually excluded by strict slicing, but might appear depending on closure.
    overnight_mask = (df_et.hour < 9) | (df_et.hour >= 17)
    overnight_rows = df_et[overnight_mask]
    
    if overnight_rows.empty:
        print("    [PASS] No overnight data found (RTH filter worked).")
    else:
        print(f"    [FAIL] Found {len(overnight_rows)} rows outside RTH!")

# --- CLEANUP ---
# Uncomment to keep data for inspection
if TEST_DIR.exists(): shutil.rmtree(TEST_DIR)