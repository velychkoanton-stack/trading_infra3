from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass(slots=True)
class ExecutorBotConfig:
    bot_id: str
    environment: str
    mysql_api_file: str
    bybit_api_file: str
    telegram_api_file: str
    rules_file_path: str
    support_monitor: bool = False
    support_monitor_interval_sec: int = 10
    heartbeat_interval_sec: int = 30
    worker_loop_sec: int = 30
    executor_threads_count: int = 4
    executor_start_delay_sec: float = 5.0
    ws_stale_close_sec: int = 120
    signal_stale_sec: int = 600
    pair_state_stale_sec: int = 1800


@dataclass(slots=True)
class CandidatePair:
    uuid: str
    asset_1: str
    asset_2: str
    tp: float
    sl: float
    last_z_score: Optional[float]
    max_z_score: Optional[float]
    min_z_score: Optional[float]
    asset1_5m_vol: Optional[float]
    asset1_1h_vol: Optional[float]
    asset2_5m_vol: Optional[float]
    asset2_1h_vol: Optional[float]
    signal_this_month: int
    signal_prev_month: int
    signal_last_update_ts: datetime
    adf: Optional[float]
    p_value: Optional[float]
    hurst: Optional[float]
    hl: Optional[float]
    spread_skew: Optional[float]
    spread_kurt: Optional[float]
    beta: Optional[float]
    beta_norm: Optional[float]
    hl_spread_med: Optional[float]
    last_spread: Optional[float]
    win_rate_180: Optional[float]
    rew_risk_180: Optional[float]
    num_trades_180: Optional[int]
    total_pnl_180: Optional[float]
    expect_180: Optional[float]
    level_180: str
    quarantine_until: Optional[datetime]
    quarantine_reason: Optional[str]
    activity_score: Optional[float]
    pair_state_last_update_ts: datetime


@dataclass(slots=True)
class SizingResult:
    leverage: float
    exposure_asset1: float
    exposure_asset2: float
    amount_asset1: float
    amount_asset2: float
    price_asset1: float
    price_asset2: float
    total_exposure: float
    beta_norm_used: float
    controller_leg: int


@dataclass(slots=True)
class OpenPairRecord:
    uuid: str
    bot_id: str
    trade_res_id: int
    asset_1: str
    asset_2: str
    ccxt_symbol_1: str
    ccxt_symbol_2: str
    pybit_symbol_1: str
    pybit_symbol_2: str
    side_1: str
    side_2: str
    open_ts: datetime
    initial_exposure: float
    leverage: float
    entry_z_score: float
    hl_bars_at_open: int
    hl_timeout_dt: datetime
    status: str = "OPEN"
    last_update_ts: datetime = field(default_factory=datetime.now)


@dataclass(slots=True)
class PairLiveMetrics:
    uuid: str
    unrealized_pnl: float
    current_pos_value: float
    updated_at: datetime


@dataclass(slots=True)
class RiskDecision:
    approved: bool
    reason: str


@dataclass(slots=True)
class CloseDecision:
    should_close: bool
    reason: str


@dataclass(slots=True)
class OrderExecutionResult:
    success: bool
    message: str
    order_ids: list[str]
    filled_notional_1: float = 0.0
    filled_notional_2: float = 0.0