import pandas as pd
from models import Order, Side, OrderType
from .strategy_base import Strategy

class MeanReversionStrategy(Strategy):
    """
    A simple Mean Reversion strategy.
    Long when price is 2 standard deviations below the mean.
    Short when price is 2 standard deviations above the mean.
    """
    def __init__(self, window=20, std_devs=2.0):
            self.window = window
            self.std_devs = std_devs
            self.prices = [] 

    def on_bar(self, bar: pd.Series):
        # 1. Update State
        current_price = bar['close']
        timestamp = bar.name
        symbol = bar['symbol']
        
        self.prices.append(current_price)
        
        if len(self.prices) > self.window:
            self.prices.pop(0)
            
        if len(self.prices) < self.window:
            return

        # 2. Get Real-Time Data from Engine
        # TRUTH SOURCE: Check what we actually own
        portfolio_item = self.engine.portfolio.get(symbol)
        current_qty = portfolio_item.position_qty if portfolio_item else 0.0
        
        # TRUTH SOURCE: Check how much buying power we have
        funds = self.engine.get_available_funds()

        # 3. Calculate Indicators
        series = pd.Series(self.prices)
        sma = series.mean()
        std = series.std()
        
        upper_band = sma + (std * self.std_devs)
        lower_band = sma - (std * self.std_devs)

        # 4. Generate Signals
        orders = []
        
        # --- ENTRY: OVERSOLD (Buy) ---
        # Logic: Price is low AND we are effectively flat (qty is near 0)
        if current_price < lower_band and abs(current_qty) < 0.0001:
            
            # DYNAMIC SIZING: Use 98% of funds to leave room for fees
            # Floor division ensures we don't try to buy 0.99999 of a share if not supported
            qty_to_buy = (funds * 0.98) / current_price
            
            # Crypto often allows decimals, but let's round to 4 places to be safe
            qty_to_buy = int(qty_to_buy * 10000) / 10000.0

            if qty_to_buy > 0:
                print(f"[{timestamp}] BUY SIGNAL. Cash: ${funds:,.2f} -> Buying {qty_to_buy} units.")
                orders.append(Order(
                    symbol=symbol,
                    side=Side.LONG,
                    order_type=OrderType.MARKET,
                    qty=qty_to_buy, 
                    strategy_name="MeanRev"
                ))
            else:
                print(f"[{timestamp}] SIGNAL IGNORED: Insufficient funds (${funds:.2f})")

        # --- ENTRY: OVERBOUGHT (Short) ---
        elif current_price > upper_band and abs(current_qty) < 0.0001:
            
            # Sizing for short is similar (assuming margin allows 1x short)
            qty_to_sell = (funds * 0.98) / current_price
            qty_to_sell = int(qty_to_sell * 10000) / 10000.0

            if qty_to_sell > 0:
                print(f"[{timestamp}] SELL SIGNAL. Cash: ${funds:,.2f} -> Shorting {qty_to_sell} units.")
                orders.append(Order(
                    symbol=symbol,
                    side=Side.SHORT,
                    order_type=OrderType.MARKET,
                    qty=qty_to_sell,
                    strategy_name="MeanRev"
                ))

        # --- EXIT: Revert to Mean (Close Long) ---
        elif current_qty > 0.0001 and current_price >= sma:
            print(f"[{timestamp}] EXIT LONG. Closing {current_qty} units.")
            orders.append(Order(
                symbol=symbol,
                side=Side.SHORT, # Sell to Close
                order_type=OrderType.MARKET,
                qty=abs(current_qty), # CLOSE EXACTLY WHAT WE HAVE
                strategy_name="MeanRev"
            ))
            
        # --- EXIT: Revert to Mean (Close Short) ---
        elif current_qty < -0.0001 and current_price <= sma:
            print(f"[{timestamp}] EXIT SHORT. Closing {abs(current_qty)} units.")
            orders.append(Order(
                symbol=symbol,
                side=Side.LONG, # Buy to Close
                order_type=OrderType.MARKET,
                qty=abs(current_qty), # CLOSE EXACTLY WHAT WE HAVE
                strategy_name="MeanRev"
            ))

        # 5. Submit
        if orders:
            if not isinstance(timestamp, pd.Timestamp):
                 # Simple Type Guard for Pylance
                 raise ValueError(f"Index must be a Timestamp")
            self.engine.submit_order(orders, timestamp)