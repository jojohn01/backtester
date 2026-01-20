from __future__ import annotations

from enum import StrEnum, auto
from datetime import datetime, timezone
from pydantic import BaseModel, model_validator



class Source(StrEnum):
    BINANCE = auto()
    KRAKEN = auto()


class AssetType(StrEnum):
    EQUITY = auto()
    ETF = auto()
    CRYPTO = auto()

class DataType(StrEnum):
    BARS = auto()
    TRADES = auto()
    QUOTES = auto()
    ORDERBOOK = auto()


class DataSpec(BaseModel):
    symbol: str
    currency: str
    source: Source
    asset_type: AssetType
    data_type: DataType = DataType.BARS
    timeframe: str | None = None
    start: datetime 
    end: datetime | None = None
    
    proxy: DataSpec | None = None
    proxy_tag: bool | None = None
    
    calendar: str | None = "NYSE"
    use_rth: bool = False # Not as useful as you would think

    rth_pad_open: int = 0 #depreciated
    rth_pad_close: int = 0 #deprecieted


    class Config:
        frozen = True


    # --- Validation Logic ---
    @model_validator(mode='after')
    def check_dates(self):
        # 1. Ensure start is before end
        if self.end and self.start >= self.end:
            raise ValueError(f"Start date ({self.start}) must be before end date ({self.end})")
        
        # 2. Ensure start is not in the future
        if self.start > datetime.now():
             raise ValueError("Start date cannot be in the future")
             
        return self
