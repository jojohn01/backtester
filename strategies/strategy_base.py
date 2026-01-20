from abc import ABC, abstractmethod
from typing import List, Dict, Optional, cast
import pandas as pd
from models import Order, Side, OrderType, AssetVars

from typing import TYPE_CHECKING

# This block only runs when your IDE is checking code, not when Python runs it.
if TYPE_CHECKING:
    from engine import ExecutionEngine


class Strategy(ABC):
    def __init__(self, name: str, symbols: List[str], engine: 'ExecutionEngine'):
        self.name = name
        self.symbols = symbols
        self.engine= engine


    @abstractmethod
    def on_bar(self, bar: pd.Series):
        pass

    def cancel_all(self, engine):
        engine.cancel_all_orders()
