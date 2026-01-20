from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, StrEnum
from typing import Optional, Dict
import uuid


class Side(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"

class Status(Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELED = "CANCELLED"



@dataclass
class Order:
    strategy_name: str
    symbol: str
    side: Side
    order_type: OrderType
    qty: float = 0.0
    cash_amount: float = 0.0
    price: Optional[float] = None
    tags: Dict[str, str] = field(default_factory=dict)

    stop_price: Optional[float] = None
    limit_price: Optional[float] = None
    stop_loss_pct: Optional[float] = None
    limit_pct: Optional[float] = None
    stop_qty: Optional[float] = None
    limit_qty: Optional[float] = None

    revenge: float = 0.0

    parent_id: Optional[str] = None
    group_id: Optional[str] = None


    commission: float = 0.0
    swap: float = 0.0

    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    status: Status = Status.PENDING
    created_at: Optional[datetime] = None
    filled_at: Optional[datetime] = None
    fill_price: float = 0.0

    def __post_init__(self):
        if self.order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT] and self.price is None:
            raise ValueError(f"Limit Orders must have a price. Symbol: {self.symbol}")
                    
        if self.order_type in [OrderType.STOP, OrderType.STOP_LIMIT] and self.price is None:
            raise ValueError(f"Stop Orders must have a price. Symbol: {self.symbol}")
        
        if self.qty <= 0 and self.cash_amount <= 0:
            raise ValueError(f"Order must have positive qty OR cash_amount. Symbol: {self.symbol}")
        
        if self.qty > 0 and self.cash_amount > 0:
            raise ValueError("Ambiguous Order: Cannot specify both qty and cash_amount.")
        
        if self.stop_loss_pct and self.stop_price:
            raise ValueError(f"Cannot specify both stop_loss_pct and stop_price.")
        
        if self.limit_pct and self.limit_price:
            raise ValueError(f"Cannot specify both limit_pct and limit_price.")
         



@dataclass
class Trade:
    trade_id: str
    order_id: str
    symbol: str
    side: Side
    qty: float
    price: float
    commission: float
    time: datetime
    pnl: float = 0.0

@dataclass
class AssetVars:
    symbol: str
    position_qty: float = 0.0
    avg_entry_price: float = 0.0
    last_price: float = 0.0
    market_fee_bps: float = 0.0
    limit_fee_bps: float = 0.0
