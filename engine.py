import pandas as pd
import numpy as np
from models import Side, Status, Trade, Order, OrderType, AssetVars
from strategy import Strategy, GapRegimeEventStrategy
from typing import List, Optional, Dict, cast
import uuid


q_epsilon = 1e-9

class ExecutionEngine:
    def __init__(self, initial_balance: float= 10000.0, portfolio: Dict[str, AssetVars] = {}, margin: float = 1.0):
        self.balance = self.equity = self.initial = initial_balance
        self.leverage = 1.0
        self.portfolio = portfolio
        self.margin = margin
        self.last_bar = None


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
    
    def run(self, data: pd.DataFrame, strategy: 'Strategy'):

        print(f"--- Engine Starting Replay: {len(data)} bars ---")

        for timestamp, row in data.iterrows():

            self.process_bar(row)
            strategy.on_bar(row)

    def process_bar(self, row: pd.Series):
        timestamp = row.name
        timestamp = cast(pd.Timestamp, timestamp)
        open_p = row['open']
        high_p = row['high']
        low_p = row['low']
        symbol_name = row['symbol']
        close_p = row['close']


        if symbol_name not in self.portfolio:
            self.portfolio[symbol_name] = AssetVars(symbol=symbol_name)

        self.portfolio[symbol_name].last_price = close_p
        asset = self.portfolio[symbol_name]


        for order in self.open_orders:
            
            if order.symbol != symbol_name or order.status != Status.PENDING:
                continue

            fill_price = self._check_fill(order, open_p, high_p, low_p)

            if fill_price is not None:
                self._execute_fill(order, fill_price, timestamp)


                if order.group_id:
                    self._cancel_group(order.group_id)

        close_p = row['close']
        unrealized_pnl = 0.0
        for sym, asset in self.portfolio.items():            
            if asset.position_qty != 0:
                current_price = close_p if sym == symbol_name else asset.last_price

                pnl = (current_price - asset.avg_entry_price) * asset.position_qty
                unrealized_pnl += pnl

        self.equity = self.get_equity()
        self.equity_curve.append({'time': timestamp, 'equity': self.equity})
        self.cleanup_orders()

    def _check_fill(self, order: Order, open_p: float, high_p: float, low_p: float) -> float | None:
        if order.order_type == OrderType.MARKET:
            if order.cash_amount:
                order.qty = order.cash_amount / open_p
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
                if open_p  < order.price:
                    return open_p
                if low_p <= order.price:
                    return order.price
            
            if order.side == Side.LONG:
                if open_p > order.price:
                    return open_p
                if high_p >= order.price:
                    return order.price
        
        return None
    
    def cleanup_orders(self):
        self.open_orders = [o for o in self.open_orders if o.status == Status.PENDING]

    def register_asset(self, asset: AssetVars):
        if asset.symbol not in self.portfolio:
            self.portfolio[asset.symbol] = asset
    
    def _execute_fill(self, order: Order, price: float, time: pd.Timestamp):
        asset = self.portfolio[order.symbol]
        notional = order.qty * price
        fee = notional * (asset.limit_fee_bps / 10000.0 if order.order_type == OrderType.LIMIT else asset.market_fee_bps / 10000.0)

        fee_per_share = fee / order.qty if order.qty > 0 else 0.0

        realized_pnl = 0.0

        new_trades = []

        if order.side == Side.LONG:
            self.balance -= (notional + fee)
            
            if asset.position_qty >= 0:
                new_qty = asset.position_qty + order.qty
                current_val = asset.position_qty * asset.avg_entry_price
                fill_val = order.qty * price
                asset.avg_entry_price = (current_val + fill_val) / new_qty
                asset.position_qty = new_qty
                new_trades.append(Trade(
                    trade_id=str(uuid.uuid4())[:8],
                    order_id=order.id,
                    symbol=order.symbol,
                    side=Side.LONG,
                    qty=order.qty,
                    price=price,
                    commission=fee,
                    time=time,
                    pnl=0.0 
                ))
            else:
                qty_closing = min(abs(asset.position_qty), order.qty)
                pnl = (asset.avg_entry_price - price) * qty_closing
                realized_pnl += pnl
                remaining_short = abs(asset.position_qty) - order.qty

                new_trades.append(Trade(
                    trade_id=str(uuid.uuid4())[:8],
                    order_id=order.id,
                    symbol=order.symbol,
                    side=Side.LONG,
                    qty=qty_closing,
                    price=price,
                    commission=qty_closing * fee_per_share,
                    time=time,
                    pnl=pnl # Realized PnL attached here
                ))


                if -q_epsilon < remaining_short < q_epsilon:
                    remaining_short = 0
                if remaining_short < 0:
                    asset.position_qty = abs(remaining_short)
                    asset.avg_entry_price = price

                    new_trades.append(Trade(
                        trade_id=str(uuid.uuid4())[:8],
                        order_id=order.id,
                        symbol=order.symbol,
                        side=Side.LONG,
                        qty=asset.position_qty,
                        price=price,
                        commission=asset.position_qty * fee_per_share,
                        time=time,
                        pnl=0.0 # New position has 0 realized PnL
                    ))
                    
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

                new_trades.append(Trade(
                    trade_id=str(uuid.uuid4())[:8],
                    order_id=order.id,
                    symbol=order.symbol,
                    side=Side.SHORT,
                    qty=order.qty,
                    price=price,
                    commission=fee,
                    time=time,
                    pnl=0.0
                ))

            else:
                qty_closing = min(asset.position_qty, order.qty)

                pnl = (price - asset.avg_entry_price) * qty_closing
                realized_pnl += pnl
                remaining_long = asset.position_qty - order.qty

                new_trades.append(Trade(
                    trade_id=str(uuid.uuid4())[:8],
                    order_id=order.id,
                    symbol=order.symbol,
                    side=Side.SHORT,
                    qty=qty_closing,
                    price=price,
                    commission=qty_closing * fee_per_share,
                    time=time,
                    pnl=pnl
                ))

                if 0 < remaining_long < q_epsilon:
                    remaining_long = 0
                if remaining_long < 0:
                    asset.position_qty = remaining_long
                    asset.avg_entry_price = price
                    new_trades.append(Trade(
                        trade_id=str(uuid.uuid4())[:8],
                        order_id=order.id,
                        symbol=order.symbol,
                        side=Side.SHORT,
                        qty=abs(remaining_long),
                        price=price,
                        commission=abs(asset.position_qty) * fee_per_share,
                        time=time,
                        pnl=0.0
                    ))
                elif remaining_long ==  0:
                    asset.position_qty = 0.0
                    asset.avg_entry_price = 0.0
                else:
                    asset.position_qty -= order.qty


             

        order.status = Status.FILLED
        order.filled_at = time
        order.fill_price = price

        self.fill_history.extend(new_trades)

        if order.stop_price or order.stop_loss_pct:
            stop_side = Side.SHORT if order.side == Side.LONG else Side.LONG
            if order.stop_price:
                sl_price = order.stop_price
            elif stop_side == Side.SHORT:
                sl_pct = cast(float, order.stop_loss_pct)
                sl_price = price * (1 - sl_pct)
            else:
               sl_pct = cast(float, order.stop_loss_pct)
               sl_price = price * (1 + sl_pct)
            

            stop_order = Order(
                strategy_name = order.strategy_name,
                symbol = order.symbol,
                side=stop_side,
                order_type=OrderType.STOP,
                price=sl_price,
                qty=order.stop_qty or (order.qty * (1 + order.revenge)),
                group_id=order.group_id or order.id
            )

            stop_order.status = Status.PENDING
            stop_order.created_at = time
            self.open_orders.append(stop_order)
        
        if order.limit_price or order.limit_pct:
            limit_side = Side.SHORT if order.side == Side.LONG else Side.LONG
            if order.limit_price:
                limit_price = order.limit_price
            elif limit_side == Side.SHORT:
                limit_pct = cast(float, order.limit_pct)
                limit_price = price * (1 + limit_pct)
            else:
               limit_pct = cast(float, order.limit_pct)
               limit_price = price * (1 - limit_pct)
            
            limit_order = Order(
                strategy_name = order.strategy_name,
                symbol = order.symbol,
                side=limit_side,
                order_type=OrderType.LIMIT,
                price=limit_price,
                qty=order.limit_qty or order.qty,
                group_id=order.group_id or order.id
            )

            limit_order.status = Status.PENDING
            limit_order.created_at = time
            self.open_orders.append(limit_order)
                

    def _cancel_group(self, group_id: str):
        for o in self.open_orders:
                if o.group_id == group_id and o.status == Status.PENDING:
                    o.status = Status.CANCELED


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
    
    def flatten(self, current_time: pd.Timestamp, symbols: Optional[List[str]] = None):

        if symbols is None:
            targets = list(self.portfolio.keys())
        else:
            targets = symbols

        for i in range(len(self.open_orders) -1, -1, -1):
            order = self.open_orders[i]
            if order.symbol in targets:
                order.status  = Status.CANCELED
                self.open_orders.pop(i)
        
        close_orders = []

        for sym in targets:
            if sym not in self.portfolio:
                continue

            asset = self.portfolio[sym]
            qty = asset.position_qty

            if abs(qty) < q_epsilon:
                continue

            side = Side.SHORT if qty > 0 else Side.LONG

            order = Order(
                symbol = sym,
                strategy_name='flatten',
                side=side,
                order_type = OrderType.MARKET,
                qty=abs(qty)
            )
            close_orders.append(order)

        if close_orders:
            self.submit_order(close_orders, current_time)

    def get_equity(self) -> float:
        """
        Calculates Total Equity = Cash Balance + Market Value of all positions.
        
        Formula:
        Equity = Cash + Sum(Position_Qty * Current_Price)
        
        * Longs (Pos > 0) ADD to equity.
        * Shorts (Pos < 0) SUBTRACT from equity (Liability).
        """
        market_value = 0.0
        
        for asset in self.portfolio.values():
            if asset.position_qty != 0:
                # asset.last_price is updated in process_bar from the current row
                market_value += (asset.position_qty * asset.last_price)
                
        return self.balance + market_value 
        
        
            
