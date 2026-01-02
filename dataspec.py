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

    class Config:
        frozen = True


    # --- Validation Logic ---
    @model_validator(mode='after')
    def check_dates(self):
        # 1. Ensure start is before end
        if self.end and self.start >= self.end:
            raise ValueError(f"Start date ({self.start}) must be before end date ({self.end})")
        
        # 2. Ensure start is not in the future
        if self.start > datetime.now(timezone.utc):
             raise ValueError("Start date cannot be in the future")
             
        return self
