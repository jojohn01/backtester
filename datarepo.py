import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataspec import DataSpec
from fetcher import DataFetcher
from typing import cast
import exchange_calendars as xcals


class DataRepository:
    def __init__(self, root_dir: str = "./data"):
        self.root = Path(root_dir)
        self.fetcher = DataFetcher()
        self._calendars = {}

    def _get_calendar(self, name: str):
        if name not in self._calendars:
            self._calendars[name] = xcals.get_calendar(name)
        return self._calendars[name]

    def _filter_calendar_rth(self, df: pd.DataFrame, spec: DataSpec) -> pd.DataFrame:
        if df.empty: return df

        cal = self._get_calendar(spec.calendar if spec.calendar else "XNYS")

        start_date = df.index.min()
        end_date = df.index.max()

        schedule = cal.schedule[start_date.replace(tzinfo=None): end_date.replace(tzinfo=None)]
        if schedule.empty:
            return pd.DataFrame()

        opens = schedule['open'].dt.tz_convert(timezone.utc) - pd.Timedelta(minutes=spec.rth_pad_open)
        closes = schedule['close'].dt.tz_convert(timezone.utc) + pd.Timedelta(minutes=spec.rth_pad_close)

        slices = []

        for i in range(len(schedule)):
            t_open = opens.iloc[i]
            t_close = closes.iloc[i]


            if t_close < df.index[0] or t_open > df.index[-1]:
                continue

            day_slice = df.loc[(df.index >= t_open) & (df.index < t_close)]

            if not day_slice.empty:
                slices.append(day_slice)
        if not slices:
            return pd.DataFrame()
        
        return pd.concat(slices).sort_index()


    def load_data(self, spec: DataSpec) -> pd.DataFrame:
        """
                Main entry point.
                1. Expands request to full days (Snap-to-Grid).
                2. Checks disk for missing ranges (Gaps).
                3. Downloads missing days/years.
                4. Returns the exact range requested, with gaps filled (0 volume).
                """
        
        target_spec = spec.proxy if (spec.proxy and spec.proxy_tag) else spec

        asset_folder = (
            self.root / target_spec.source.value / 
            target_spec.asset_type.value / target_spec.data_type.value /
            target_spec.symbol / target_spec.currency / (target_spec.timeframe if target_spec.timeframe else '1m')
        )

        asset_folder.mkdir(parents= True, exist_ok= True)
        
        buffer = timedelta(minutes=max(spec.rth_pad_open, spec.rth_pad_close, 60))


        load_start = self._floor_date(spec.start - buffer)
        if spec.end:
            load_end = self._ciel_date(spec.end + buffer)
        else:
            load_end = datetime.now(timezone.utc)
        
        load_spec = target_spec.model_copy(update={'start': load_start, 'end': load_end})
        stored_intervals = self._scan_stored_intervals(asset_folder, load_spec)
        gaps = self._find_gaps(load_start, load_end, stored_intervals)

        for gap_start, gap_end in gaps:
            fetch_spec = target_spec.model_copy(update={'start': gap_start, 'end': gap_end})
            new_data = self.fetcher.fetch(fetch_spec)
            if new_data is not None and not new_data.empty:
                self._save_partitioned(new_data, asset_folder)

        full_df = self._load_from_folder(asset_folder, load_start, load_end)
        if full_df.empty: return pd.DataFrame()

        full_df = self._fill_gaps(full_df, spec.timeframe)

        if spec.use_rth:
            full_df = self._filter_calendar_rth(full_df, spec)

        mask = (full_df.index >= spec.start)
        if spec.end:
            mask &= (full_df.index < spec.end)

        return full_df.loc[mask].copy()




    def _scan_stored_intervals(self, folder: Path, spec: DataSpec) -> list[tuple[datetime, datetime]]:
            
            intervals = []

            freq_map = {'1m': '1min', '3m': '3min', '5m': '5min', '10m': '10min', '15m': '15min', '30m': '30min', '1h': '1h', '2h': '2h', '4h': '4h', '6h': '6h', '8h': '8h', '12h': '12h', '1d': '1D', '3d': '3D', '1w': '1W'}
            freq = freq_map.get(spec.timeframe, '1min') if spec.timeframe else '1min'
            gap_threshold = pd.Timedelta(freq) * 1.5
            start_year = spec.start.year
            end_year = spec.end.year if spec.end else datetime.now().year

            # 1. Collect raw intervals from files
            for file_path in sorted(folder.glob("*.parquet")):
                try:
                    file_year = int(file_path.stem)
                    if file_year < start_year or file_year > end_year:
                        continue
                except ValueError:
                    pass

                try:
                    filters = [("timestamp", ">=", spec.start)]
                    
                    df = pd.read_parquet(
                        file_path, 
                        columns=['timestamp'], 
                        engine='pyarrow',
                        filters=filters
                    )
                    if df.index.empty:
                        continue
                
                    if 'timestamp' in df.columns:
                        timestamps = df['timestamp']
                    else:
                        timestamps = df.index.to_series()

                    diffs = timestamps.sort_values().diff()

                    gap_mask = diffs > gap_threshold
                    if not gap_mask.any():
                        intervals.append((timestamps.min(), timestamps.max()))
                    else:
                        chunk_ids = gap_mask.cumsum()

                        for _, chunk in timestamps.groupby(chunk_ids):
                            intervals.append((chunk.min(), chunk.max()))

                except Exception as e:
                    print(f"[!] Error scanning file {file_path}: {e}")
                    continue

            # 2. MERGE ADJACENT INTERVALS (The Fix)
            if not intervals:
                return []

            # Sort by start time
            intervals.sort(key=lambda x: x[0])
            
            merged = []
            curr_start, curr_end = intervals[0]

            for next_start, next_end in intervals[1:]:
                # If the next chunk starts within the threshold of the current chunk's end
                # (e.g. 23:45 + 15m threshold >= 00:00), we merge them.
                if next_start <= curr_end + gap_threshold:
                    curr_end = max(curr_end, next_end)
                else:
                    merged.append((curr_start, curr_end))
                    curr_start, curr_end = next_start, next_end
            
            merged.append((curr_start, curr_end))
            
            return merged

    def _find_gaps(self, req_start: datetime, req_end: datetime, existing_intervals: list) -> list[tuple]:
        gaps = []
        current_pointer = req_start

        for (exist_start, exist_end) in existing_intervals:
            if exist_start.tzinfo is None:
                exist_start = exist_start.replace(tzinfo=timezone.utc)
            if exist_end.tzinfo is None:
                exist_end = exist_end.replace(tzinfo=timezone.utc)

            if exist_end < current_pointer:
                continue

            if exist_start > current_pointer:
                gaps.append((current_pointer, exist_start))

            current_pointer = max(current_pointer, exist_end)

            if current_pointer >= req_end:
                break

        if current_pointer < req_end:
            gaps.append((current_pointer, req_end))
        
        return gaps

    def _save_partitioned(self, df: pd.DataFrame, folder: Path):
            if df.empty:
                return
            
            idx = cast(pd.DatetimeIndex, df.index)
            for year, year_df in df.groupby(idx.year):
                file_path = folder / f"{year}.parquet"
                
                try: # Add Try/Except specifically here
                    if file_path.exists():
                        existing = pd.read_parquet(file_path)
                       
                        combined = pd.concat([existing, year_df])
                        combined = combined[~combined.index.duplicated(keep='last')]
                        combined.sort_index(inplace=True)
                        
                        combined.to_parquet(file_path, engine='pyarrow', compression='snappy', index=True)
                    else:
                        year_df.to_parquet(file_path, engine='pyarrow', compression='snappy', index=True)
                        
                except Exception as e:
                    print(f"    [!!!] CRASH SAVING YEAR {year}: {e}")
                    import traceback
                    traceback.print_exc()
                    raise e

    def _load_from_folder(self, folder: Path, start: datetime, end: datetime) -> pd.DataFrame:
            """
            Reads the FOLDER as a single dataset.
            Uses Pushdown Filters to skip reading irrelevant row groups.
            """
            
            if not folder.exists():
                print("    [DEBUG] Folder does not exist.")
                return pd.DataFrame()
                
            # sanity check: are there files?
            files = list(folder.glob("*.parquet"))
            
            if not files:
                print("    [DEBUG] No parquet files found.")
                return pd.DataFrame()

            try:
                return pd.read_parquet(
                    folder, 
                    engine='pyarrow',
                    filters=[
                        ('timestamp', '>=', start),
                        ('timestamp', '<', end)
                    ]
                )

            except Exception as e:
                # Catch EVERYTHING to see why it crashes
                print(f"    [!!!] CRASH IN _load_from_folder: {e}")
                import traceback
                traceback.print_exc()
                return pd.DataFrame()

    def _fill_gaps(self, df: pd.DataFrame, timeframe: str | None) -> pd.DataFrame:
        if df.empty: return df
        freq_map = {'1m': '1min', '3m': '3min', '5m': '5min', '10m': '10min', '15m': '15min', '30m': '30min', '1h': '1h', '2h': '2h', '4h': '4h', '6h': '6h', '8h': '8h', '12h': '12h', '1d': '1D', '3d': '3D', '1w': '1W'}     
        timeframe = '1m' if timeframe is None else timeframe
        freq = freq_map.get(timeframe, '1min')

        idx = cast(pd.DatetimeIndex, df.index)

        full_index = pd.date_range(start=idx.min(), end=idx.max(), freq=freq, tz=idx.tz)

        df = df.reindex(full_index)

        df['close'] = df['close'].ffill()
        df['open'] = df['open'].fillna(df['close'])
        df['high'] = df['high'].fillna(df['close'])
        df['low'] = df['low'].fillna(df['close'])
        df['volume'] = df['volume'].fillna(0)

        return df



    def _floor_date(self, dt: datetime) -> datetime:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    def _ciel_date(self, dt: datetime) -> datetime:
        return (dt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)