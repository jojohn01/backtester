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
    qty: float
    price: Optional[float] = None
    tags: Dict[str, str] = field(default_factory=dict)

    stop_price: Optional[float] = None
    parent_id: Optional[str] = None
    group_id: Optional[str] = None


    commission: float = 0.0
    swap: float = 0.0

    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    status: Status = Status.PENDING
    created_at: Optional[datetime] = None
    filled_at: Optional[datetime] = None
    fill_price: float = 0.0    


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