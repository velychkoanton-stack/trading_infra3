# Trading Infrastructure for Crypto Statistical Arbitrage

Scalable trading infrastructure for mean-reversion strategies on crypto perpetual futures.

Designed to support continuous pair discovery, statistical validation, and multi-threaded execution across hundreds of trading pairs.

---

## Overview

This project is a full-stack trading infrastructure built from scratch for statistical arbitrage (pair trading).

Unlike a simple trading bot, the system is structured as a **multi-layer pipeline**:

- continuous research (pair discovery)
- live signal generation
- execution with risk management

The architecture is designed to scale up to **~1000 trading pairs** while maintaining stability, modularity, and low database contention.

---

## System Architecture

Full system design (Miro):

https://miro.com/app/board/uXjVGvMAis4=/?share_link_id=738051014447

https://drive.google.com/file/d/1EMayTe_lllYPqtKasfjsl28beDMMTTTA/view?usp=sharing


The system is split into three main layers:

### 1. Selection Layer
Responsible for continuous discovery and validation of trading pairs.

- Asset universe construction
- Liquidity filtering
- Stationarity tests (ADF)
- Pair generation
- Statistical validation:
  - ADF (spread)
  - Hurst exponent
  - Half-life
  - skew / kurtosis
  - beta estimation
- Backtesting
- Candidate selection

---

### 2. Working Layer
Maintains live state and produces trading signals.

- Spread calculation
- Z-score computation
- Cointegration monitoring
- Rolling performance tracking (30d / 180d)
- Pair classification (Quarantine / Level 0 / Level 1 / Level 2)
- Asset locking (prevent multi-position conflicts)
- Trade result tracking
- Live PnL monitoring
- Daily balance snapshots

---

### 3. Execution Layer
Handles trading logic and system coordination.

- Scheduler (run/sleep control, weekend shutdown)
- Bot heartbeat monitoring
- Multi-thread execution bots
- Real-time PnL tracking via WebSocket
- Trade lifecycle management

---

## Data & Pipeline

The system uses a hybrid data architecture:

- **MySQL** → structured state & metadata
- **Parquet** → historical OHLCV storage
- **Workers** → continuous data processing

Pipeline flow:

1. Asset discovery → asset_universe  
2. Pair creation → pair_universe  
3. Statistical validation → candidate pairs  
4. Signal generation → signal table  
5. Execution → trades + results  

All workers operate independently and continuously.

---

## Strategy

Core strategy: **mean reversion via pair trading**

Key concepts:

- Z-score based entry signals
- Cointegration validation
- Half-life-based holding logic
- Spread normalization
- Multi-condition exits

Entry:
- Z-score threshold breach + Cointegration validation

Exit:
- Mean reversion (Z → 0)
- Stop-loss / Take-profit
- Extreme divergence
- Half-life timeout
- Cointegration loss

---

## Execution Engine

Execution is handled by multi-threaded bots:

- Each bot runs several threads
- WebSocket connection for live data:
  - positions
  - unrealized PnL
  - balance
- Local state used for fast decision-making

Key mechanics:

- Asset-level locking (no duplicate exposure)
- Trade lifecycle tracking
- Fail-safe closing logic
- Independent bot operation

---

## Risk Management

Risk control is embedded inside execution:

- Daily loss limits
- Pair-level TP / SL
- Cointegration-based exits
- Extreme Z-score protection
- Connection failure handling

The system is designed to **fail safely** rather than maximize uptime.

---

## Dashboard & Results

Performance is tracked in a dashboard:

https://drive.google.com/file/d/1EMayTe_lllYPqtKasfjsl28beDMMTTTA/view?usp=sharing


Includes:

- PnL tracking
- win rate
- trade distribution
- pair-level performance
- risk metrics

---

### Results (on 2026-03-19 is in testing mode.)

- Total trades: XXX  
- Win rate: XX%  
- Profit factor: X.XX  
- Max drawdown: XX%  
- Return: XX%  

(*best up-to-date results in dashboard*)

---

## Tech Stack

- Python
- MySQL
- CCXT
- WebSockets (Pybit)
- Pandas / NumPy / Statsmodels
- Parquet (PyArrow)
- Power BI (analytics)

---

## Key Features

- Scalable multi-layer architecture
- Separation of research and execution
- Modular worker system
- Parquet-based data caching
- Deadlock-aware database design
- Asset-level exposure control
- Continuous pair discovery
- Real-time execution tracking

---

## Challenges & Learnings

- Designing a system that avoids database deadlocks under concurrency
- Managing consistency between asynchronous workers
- Separating research logic from execution logic
- Building reliable state tracking for live trading
- Handling partial failures (API / WS / DB)
- Balancing performance vs correctness in live systems

---

## Future Improvements

- Docker-based deployment
- Distributed worker orchestration
- Advanced regime detection
- Dynamic position sizing
- Improved execution latency
- ML-based pair scoring
- Portfolio-level optimization

---

## Author

Anton Velychko

This project was built as part of a transition into:
- quantitative trading
- trading infrastructure engineering
- data engineering

---

## Notes

This is an experimental infrastructure and is continuously evolving.
