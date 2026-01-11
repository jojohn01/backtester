import pandas as pd
import numpy as np
from models import Side, Status, Trade, Order, OrderType, AssetVars
from deprecated.strategy import Strategy
from typing import List, Optional, Dict, cast
import uuid


class ExecutionEngine:
    def __init__(self, initial_balance: float= 10000.0, portfolio: Dict[str, AssetVars] = {}, margin: float = 1.0):
        self.balance = self.equity = self.initial = initial_balance
        self.leverage = 1.0
        self.portfolio = portfolio
        self.margin = margin


        self.open_orders: List[Order] = []
        self.fill_history: List[Trade] = []
        self.equity_curve = []


    def submit_order(self, orders: List[Order], current_time: pd.Timestamp):

        for order in orders:
            order.created_at = current_time
            order.status = Status.PENDING
            self.open_orders.append(order)
    
    def cancel_all_orders(self):
        for order in self.open_orders:
            order.status = Status.CANCELED
        self.open_orders = []
    
    def process_bar(self, row: pd.Series):
        timestamp = row.name
        timestamp = cast(pd.Timestamp, timestamp)
        open_p = row['open']
        high_p = row['high']
        low_p = row['low']
        symbol_name = row['symbol']
        close_p = row['close']


        if symbol_name not in self.portfolio:
            return None

        self.portfolio[symbol_name].last_price = close_p
        asset = self.portfolio[symbol_name]


        for order in self.open_orders[:]:
            
            if order.symbol != symbol_name:
                continue

            fill_price = self._check_fill(order, open_p, high_p, low_p)

            if fill_price is not None:
                self._execute_fill(order, fill_price, timestamp)
                self.open_orders.remove(order)

                if order.group_id:
                    self._cancel_group(order.group_id)

        close_p = row['close']
        unrealized_pnl = 0.0
        for sym, asset in self.portfolio.items():            
            if asset.position_qty != 0:
                current_price = close_p if sym == symbol_name else asset.last_price

                pnl = (current_price - asset.avg_entry_price) * asset.position_qty
                unrealized_pnl += pnl

        self.equity = self.balance + unrealized_pnl
        self.equity_curve.append({'time': timestamp, 'equity': self.equity})

    def _check_fill(self, order: Order, open_p: float, high_p: float, low_p: float) -> float | None:
        if order.order_type == OrderType.MARKET:
            return open_p

        order.price = cast(float, order.price)
        order.stop_price = cast(float, order.stop_price)
        if order.order_type == OrderType.LIMIT:
            if order.side == Side.LONG and low_p <= order.price:
                return min(open_p, order.price)
            
            if order.side == Side.SHORT and high_p >= order.price:
                return max(open_p, order.price)
            
        if order.order_type == OrderType.STOP:
            if order.side == Side.SHORT:
                if open_p  < order.stop_price:
                    return open_p
                if low_p <= order.stop_price:
                    return order.stop_price
            
            if order.side == Side.LONG:
                if open_p > order.stop_price:
                    return open_p
                if high_p >= order.stop_price:
                    return order.stop_price
        
        return None
    
    def register_asset(self, asset: AssetVars):
        if asset.symbol not in self.portfolio:
            self.portfolio[asset.symbol] = asset
    
    def _execute_fill(self, order: Order, price: float, time: pd.Timestamp):
        asset = self.portfolio[order.symbol]
        notional = order.qty * price
        fee = notional * (asset.limit_fee_bps / 10000.0 if order.order_type == OrderType.LIMIT else asset.market_fee_bps / 10000.0)

        realized_pnl = 0.0

        if order.side == Side.LONG:
            self.balance -= (notional + fee)
            
            if asset.position_qty >= 0:
                new_qty = asset.position_qty + order.qty
                current_val = asset.position_qty * asset.avg_entry_price
                fill_val = order.qty * price
                asset.avg_entry_price = (current_val + fill_val) / new_qty
                asset.position_qty = new_qty
            else:
                qty_closing = min(abs(asset.position_qty), order.qty)
                pnl = (asset.avg_entry_price - price) * qty_closing
                realized_pnl += pnl
                remaining_short = abs(asset.position_qty) - order.qty
                if remaining_short < 0:
                    asset.position_qty = abs(remaining_short)
                    asset.avg_entry_price = price
                elif remaining_short == 0:
                    asset.position_qty = 0.0
                    asset.avg_entry_price = 0.0
                else:
                    asset.position_qty += order.qty
        
        elif order.side == Side.SHORT:
            self.balance += (notional - fee)


            if asset.position_qty <= 0:
                current_qty = abs(asset.position_qty)
                fill_qty = order.qty
                new_qty = current_qty + fill_qty

                current_val = current_qty * asset.avg_entry_price
                fill_val = fill_qty * price
                asset.avg_entry_price = (current_val + fill_val) / new_qty
                asset.position_qty -= order.qty

            else:
                qty_closing = min(asset.position_qty, order.qty)

                pnl = (price - asset.avg_entry_price) * qty_closing
                realized_pnl += pnl
                remaining_long = asset.position_qty - order.qty

                if remaining_long < 0:
                    asset.position_qty = remaining_long
                    asset.avg_entry_price = price
                elif remaining_long ==  0:
                    asset.position_qty = 0.0
                    asset.avg_entry_price = 0.0
                else:
                    asset.position_qty -= order.qty

             

        order.status = Status.FILLED
        order.filled_at = time
        order.fill_price = price

        t = Trade(
            trade_id = str(uuid.uuid4())[:8],
            order_id = order.id,
            symbol = order.symbol,
            side = order.side,
            qty = order.qty,
            price=price,
            commission=fee,
            time=time,
            pnl=realized_pnl
        )

        self.fill_history.append(t)

    def _cancel_group(self, group_id: str):
        for i in range(len(self.open_orders) - 1, -1, -1):
            o = self.open_orders[i]
            if o.group_id == group_id:
                self.open_orders.pop(i)

    def get_available_funds(self):
        total = self.balance
        debt = 0
        for k, a in self.portfolio.items():
            if a.position_qty < 0.0:
                debt += a.avg_entry_price  * abs(a.position_qty)
        total -= 2*debt
        return (1/self.margin)*total
    
    def get_initial(self):
        return self.initial