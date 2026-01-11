import pandas as pd
from dataspec import DataSpec, Source, AssetType, DataType
import ccxt
from datetime import datetime, timezone
from pydantic import BaseModel, model_validator
import time
from tqdm import tqdm
import traceback


def fetch_ohlcv_range(exchange, spec: DataSpec) -> pd.DataFrame:
    all_candles = []
    
    # Construct symbol (e.g. "BTC" + "/USDT" -> "BTC/USDT")
    symbol = spec.symbol + spec.currency
    timeframe = spec.timeframe
    start_time = spec.start
    end_time = spec.end

    print(f"--- Starting Download: {symbol} [{timeframe}] ---")
    print(f"--- From: {start_time} To: {end_time} ---")

    # 1. Setup Start/End in Milliseconds
    since = int(start_time.timestamp() * 1000)
    
    if end_time:
        end = int(end_time.timestamp() * 1000)
    else:
        end = int(datetime.now(timezone.utc).timestamp() * 1000)

    # 2. Main Download Loop
    while since < end:
        try:
            # Fetch the batch (Max 1000 candles per call)
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=1000)
        
        except ccxt.RateLimitExceeded:
            print(f"    [!] Rate Limit Hit on {symbol}. Sleeping 5s...")
            time.sleep(5)
            continue
            
        except Exception as e:
            print(f"\n[!!!] CRITICAL ERROR fetching {symbol} at timestamp {since}")
            print(f"Error Message: {e}")
            traceback.print_exc() # This prints the full error stack
            break # Stop the loop so we don't spam requests

        # SAFETY CHECK: Stop if exchange returns no data
        if not ohlcv:
            print(f"    [i] Exchange returned no data at {since}. Stopping.")
            break

        all_candles.extend(ohlcv)

        # Get timestamp of the last candle in this batch
        last_timestamp = ohlcv[-1][0]
        last_date_str = pd.to_datetime(last_timestamp, unit='ms', utc=True)
        
        print(f"    -> Fetched {len(ohlcv)} candles. Last: {last_date_str}")

        # UPDATE LOOP TRACKER
        # Critical: Must start *after* the last candle we just got
        prev_since = since
        since = last_timestamp + 1
        
        # SAFETY CHECK: Infinite Loop Prevention
        # If the exchange keeps returning the same candle, force break
        if since <= prev_since:
            print("    [!] Warning: Timestamp did not advance. Breaking to prevent infinite loop.")
            break
        
        # Respect Rate Limit (convert ms to seconds)
        time.sleep(exchange.rateLimit / 1000)

    # 3. Create DataFrame
    print(f"--- Download Loop Finished. Processing {len(all_candles)} rows... ---")
    
    if not all_candles:
        return pd.DataFrame()

    df = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df['symbol'] = spec.symbol
    df['symbol'] = df['symbol'].astype('category')
    df.set_index('timestamp', inplace=True)
    
    # Final filter to ensure we respect the exact end_time
    if end_time:
         df = df[df.index <= end_time]

    return df


class DataFetcher:

    def fetch(self, spec: DataSpec):
         match spec.data_type:
              case DataType.BARS:
                   return self.fetch_bars_data(spec)
              case DataType.TRADES:
                   return self.fetch_trades_data(spec)
              case DataType.QUOTES:
                   return self.fetch_quotes_data(spec)
              case DataType.ORDERBOOK:
                   return self.fetch_orderbook_data(spec)

    def fetch_trades_data(self, spec: DataSpec):
        return
    
    def fetch_quotes_data(self, spec: DataSpec):
        return
    
    def fetch_orderbook_data(self, spec: DataSpec):
        return


    def fetch_bars_data(self, spec: DataSpec) -> pd.DataFrame:
        match spec.source:
                case Source.BINANCE:
                    exchange = ccxt.binance()
                    return fetch_ohlcv_range(exchange, spec)
                case Source.KRAKEN:
                    exchange = ccxt.kraken()
                    return fetch_ohlcv_range(exchange, spec)
                case _:
                    raise ValueError(f"Unknown source: {spec.source}")

