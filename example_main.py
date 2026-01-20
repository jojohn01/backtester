import pandas as pd
from datetime import datetime, timedelta, timezone

from typing import cast, Any

# Import Architecture Components
from datarepo import DataRepository
from dataspec import DataSpec, Source, AssetType, DataType # Assuming these Enums exist
from engine import ExecutionEngine
from models import AssetVars
from strategies.MeanReversionStrategy import MeanReversionStrategy
from strategies.strategy_base import Strategy

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def plot_results(engine, symbol: str, data: pd.DataFrame):
    """
    Generates a dual-pane chart:
    1. Top: Price History with Buy/Sell markers.
    2. Bottom: Equity Curve.
    """
    print("--> Generating Performance Chart...")
    
    trades = engine.fill_history
    
    # Extract Buy/Sell points
    buys = [t for t in trades if t.side == 'LONG']
    sells = [t for t in trades if t.side == 'SHORT']
    
    # Extract Equity Curve
    equity_data = pd.DataFrame(engine.equity_curve)
    if not equity_data.empty:
        equity_data.set_index('time', inplace=True)
    
    # Setup Figure
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True, gridspec_kw={'height_ratios': [2, 1]})
    plt.subplots_adjust(hspace=0.05)
    
    # --- TOP PANEL: Price & Trades ---
    
    # 2. PLOT THE CONTINUOUS PRICE LINE (The Fix)
    # We assume 'data' index is timestamps and has a 'close' column
    ax1.plot(data.index, data['close'], label='Price', color='black', alpha=0.6, linewidth=1)
    
    # Plot Buy Markers (Green Triangle Up)
    if buys:
        buy_times = [t.time for t in buys]
        buy_prices = [t.price for t in buys]
        ax1.scatter(buy_times, buy_prices, marker='^', color='green', s=100, label='Buy', zorder=5)

    # Plot Sell Markers (Red Triangle Down)
    if sells:
        sell_times = [t.time for t in sells]
        sell_prices = [t.price for t in sells]
        ax1.scatter(sell_times, sell_prices, marker='v', color='red', s=100, label='Sell', zorder=5)
    
    ax1.set_title(f"{symbol} Trading Strategy Analysis")
    ax1.set_ylabel("Asset Price")
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # --- BOTTOM PANEL: Equity Curve ---
    if not equity_data.empty:
        ax2.plot(equity_data.index, equity_data['equity'], color='blue', linewidth=1.5)
        ax2.fill_between(equity_data.index, equity_data['equity'], equity_data['equity'].min(), alpha=0.1, color='blue')
    
    ax2.set_ylabel("Account Equity ($)")
    ax2.set_xlabel("Date")
    ax2.grid(True, alpha=0.3)
    
    # Format Date Axis
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.xticks(rotation=45)
    
    filename = "backtest_result.png"
    plt.savefig(filename)
    print(f"--> Chart saved to {filename}")
    plt.show()


def run_pipeline():
    print("==========================================")
    print("   QUANT PIPELINE: DATALINK -> ENGINE     ")
    print("==========================================")

    # ------------------------------------------------
    # 1. SETUP DATA SPECIFICATION
    # ------------------------------------------------
    # Define what we want to trade.
    # We ask for a specific week of data to keep the test fast.
    start_date = datetime(2023, 10, 23) # UTC assumed. Do not pass in timezone aware dates
    end_date = datetime(2023, 10, 27)

    spec = DataSpec(
        source=Source.BINANCE,      # or Source.POLYGON, etc.
        asset_type=AssetType.CRYPTO,  # or AssetType.CRYPTO
        data_type=DataType.BARS,
        symbol="BTC",
        currency="USDT",
        timeframe="15m",           # 15-minute bars
        start=start_date,
        end=end_date,
        use_rth=False,             # Crypto runs 24/7
        rth_pad_open=0,             # depriciated
        rth_pad_close=0             #depreciated
    )

    # ------------------------------------------------
    # 2. INITIALIZE REPOSITORY & LOAD DATA
    # ------------------------------------------------
    print(f"--> Initializing Repository at ./data")
    repo = DataRepository(root_dir="./data")
    
    print(f"--> Loading Data for {spec.symbol} ({start_date.date()} to {end_date.date()})...")
    
    # This calls your Robust Loader:
    # Snap-to-grid -> Check Disk -> Fill Gaps (Fetch) -> Filter RTH
    df = repo.load_data(spec)
    
    if df.empty:
        print("[ERROR] No data returned from Repository. Check Fetcher or Inputs.")
        return

    print(f"--> Data Loaded Successfully: {len(df)} bars.")
    print(f"    Range: {df.index.min()} to {df.index.max()}")
    
    # Ensure the dataframe has a 'symbol' column for the engine to use
    if 'symbol' not in df.columns:
        df['symbol'] = spec.symbol

    # ------------------------------------------------
    # 3. CONFIGURE ENGINE & STRATEGY
    # ------------------------------------------------
    print("--> Configuring Execution Engine...")
    
    # Set up portfolio with the asset we are trading
    portfolio = {
        spec.symbol: AssetVars(
            symbol=spec.symbol, 
            market_fee_bps=5.0 # 0.05% fee simulation
        )
    }
    
    engine = ExecutionEngine(initial_balance=100000.0, portfolio=portfolio)
    strategy = MeanReversionStrategy(window=20, std_devs=2.0)
    
    # Inject dependencies
    strategy.engine = engine

    # ------------------------------------------------
    # 4. RUN BACKTEST
    # ------------------------------------------------
    print("--> Starting Simulation...")
    strategy = cast(Any, strategy)
    engine.run(df, strategy)

    # ------------------------------------------------
    # 5. PERFORMANCE REPORT
    # ------------------------------------------------
    print("\n==========================================")
    print("           PERFORMANCE RESULTS            ")
    print("==========================================")
    
    initial = engine.get_initial()
    final = engine.equity
    pnl = final - initial
    pnl_pct = (pnl / initial) * 100
    total_trades = len(engine.fill_history)
    
    print(f"Symbol:        {spec.symbol}")
    print(f"Timeframe:     {spec.timeframe}")
    print(f"Initial Cash:  ${initial:,.2f}")
    print(f"Final Equity:  ${final:,.2f}")
    print(f"Net PnL:       ${pnl:,.2f} ({pnl_pct:.2f}%)")
    print(f"Total Trades:  {total_trades}")
    
    # Simple ASCII Equity Curve
    if engine.equity_curve:
        print("\nEquity Snapshot (First 5 vs Last 5):")
        for pt in engine.equity_curve[:5]:
            print(f"  {pt['time']}: ${pt['equity']:.2f}")
        print("  ...")
        for pt in engine.equity_curve[-5:]:
            print(f"  {pt['time']}: ${pt['equity']:.2f}")

    plot_results(engine, "BTC", df)

if __name__ == "__main__":
    run_pipeline()