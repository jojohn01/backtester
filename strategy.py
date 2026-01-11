from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import pandas as pd
from models import Order, Side, OrderType, AssetVars

class Strategy(ABC):
    def __init__(self, name: str, symbols: List[str]):
        self.name = name
        self.symbols = symbols

    @abstractmethod
    def on_bar(self, bar: pd.Series, engine_state: Dict[str, AssetVars], open_orders: List[Order], available_cash: float) -> List[Order]:
        pass

    def cancel_all(self, engine):
        engine.cancel_all_orders()
