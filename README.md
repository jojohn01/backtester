# Backtester

A Python framework for backtesting trading strategies on historical
market data.

This project allows users to simulate and evaluate trading strategies
before deploying them with real capital. It provides a modular structure
that separates strategy logic, data handling, and execution flow.

Features

Modular backtesting engine

Historical data loading and simulation

Strategy plug-and-play architecture

Example strategy implementation

Performance output including executed trades and results

Easy extensibility for additional strategies

Project Structure

backtester/ ├── strategies/ Strategy implementations ├── datarepo.py
Data loading and handling ├── engine.py Core backtesting engine ├──
strategy.py Base strategy interface ├── example_main.py Example runner
script ├── fetcher.py Market data fetching logic └── models.py Data
models

Installation

Clone the repository

git clone https://github.com/jojohn01/backtester

cd backtester

Create and activate a virtual environment

python -m venv venv source venv/bin/activate

On Windows: venv ctivate

Install dependencies

pip install -r requirements.txt

Running the Backtester

Run the example script:

python example_main.py

This will:

Load historical price data

Execute the selected strategy

Simulate trades

Output performance results

How It Works

Historical market data is loaded through the data repository module.

The engine iterates over the data one bar at a time.

A strategy processes each data point and generates buy/sell signals.

Trades are simulated and tracked.

Performance results are calculated and displayed.

Creating a Custom Strategy

Create a new strategy inside the strategies directory by subclassing the
base Strategy class.

Example:

from strategy import Strategy

class MyStrategy(Strategy): def on_bar(self, bar): pass

Implement your trading logic inside the on_bar method.

Possible Enhancements

Advanced performance metrics (Sharpe ratio, max drawdown, win rate)

Exporting results to CSV

Visualization of equity curves

Parameter optimization tools

Integration with live market data feeds
