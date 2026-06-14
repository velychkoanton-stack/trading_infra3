from __future__ import annotations

import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Common.db.db_execute import execute_many, fetch_all
from Common.db.heartbeat_writer import write_heartbeat
from Common.exchange.bybit_client import create_bybit_client, fetch_ohlcv_with_retry
from Common.parquet.parquet_reader import read_symbol_ohlcv_parquet_from_storage
from Common.parquet.parquet_writer import write_symbol_ohlcv_parquet_to_storage
from Common.utils.cleanup import force_gc
from Common.utils.logger import setup_logger
from Common.utils.sql_file_loader import load_sql_file
from Working_layer.Pair_state_worker.Pair_state_worker import PairStateWorker


VALID_SLEEP_STATUSES = {"SLEEP", "STOP", "SL_BLOCK"}


class PairStateWorker15Min(PairStateWorker):
    """
    Hourly statistical-state worker for the isolated 15-minute strategy.

    State calculations use 500 completed 1-hour candles. Trade-history
    metrics use only trade_res_15min.
    """

    def __init__(self) -> None:
        self.worker_dir = Path(__file__).resolve().parent
        self.project_root = self.worker_dir.parents[1]
        self.rules_path = self.worker_dir / "rules" / "rules.txt"
        self.sql_dir = self.worker_dir / "sql_queries"
        self.log_path = (
            self.project_root
            / "data"
            / "logs"
            / "Working_layer"
            / "Pair_state_worker_15min"
            / "pair_state_worker_15min.log"
        )
        self.logger = setup_logger(
            logger_name="WorkingLayer.PairStateWorker15Min",
            log_file_path=self.log_path,
        )
        self.rules = self._load_rules_file(self.rules_path)

        self.mysql_api_file = self.rules.get("mysql_api_file", "api_mysql_main.txt")
        self.bybit_api_file = self.rules.get("bybit_api_file", "api_bybit_main.txt")
        self.worker_name = self.rules.get(
            "worker_name", "pair_state_worker_15min"
        )
        self.timeframe = self.rules.get("timeframe", "1h")
        self.loop_minutes = int(self.rules.get("loop_minutes", "60"))
        self.loop_start_delay_sec = int(
            self.rules.get("loop_start_delay_sec", "8")
        )
        self.scheduler_sleep_check_sec = int(
            self.rules.get("scheduler_sleep_check_sec", "30")
        )
        self.lookback_candles = int(self.rules.get("lookback_candles", "500"))
        self.fetch_buffer_candles = int(
            self.rules.get("fetch_buffer_candles", "5")
        )
        self.parquet_storage_name = self.rules.get(
            "parquet_storage_name", "bybit_linear_1h_15min_test"
        )
        self.symbol_refresh_workers = int(
            self.rules.get("symbol_refresh_workers", "8")
        )

        self.adf_threshold = float(self.rules.get("adf_threshold", "-2.5"))
        self.p_value_threshold = float(
            self.rules.get("p_value_threshold", "0.10")
        )
        self.beta_raw_min = float(self.rules.get("beta_raw_min", "0.10"))
        self.quarantine_days = int(self.rules.get("quarantine_days", "14"))
        self.quarantine_losing_days_streak = int(
            self.rules.get("quarantine_losing_days_streak", "5")
        )
        self.same_event_window_minutes = int(
            self.rules.get("same_event_window_minutes", "20")
        )
        self.level2_180_min_trades = int(
            self.rules.get("level2_180_min_trades", "20")
        )
        self.level2_30_min_trades = int(
            self.rules.get("level2_30_min_trades", "5")
        )
        self.removal_min_trades = int(
            self.rules.get("removal_min_trades", "25")
        )
        self.use_log_prices_for_beta = self._parse_bool(
            self.rules.get("use_log_prices_for_beta", "1")
        )
        self.use_log_prices_for_adf = self._parse_bool(
            self.rules.get("use_log_prices_for_adf", "1")
        )
        self.min_aligned_candles = int(
            self.rules.get("min_aligned_candles", "450")
        )
        self.quarantine_recent_trade_days = 7

        self.get_pairs_sql = load_sql_file(self.sql_dir / "get_pairs.txt")
        self.insert_pair_state_sql = load_sql_file(
            self.sql_dir / "insert_pair_state.txt"
        )
        self.get_trade_results_sql = load_sql_file(
            self.sql_dir / "get_trade_results.txt"
        )
        self.update_pair_state_sql = load_sql_file(
            self.sql_dir / "update_pair_state.txt"
        )
        self.get_scheduler_statuses_sql = load_sql_file(
            self.sql_dir / "get_scheduler_statuses.txt"
        )
        self.bybit = create_bybit_client(self.bybit_api_file)

    def run_forever(self) -> None:
        self.logger.info(
            "15m pair-state worker started | state_tf=%s | lookback=%s",
            self.timeframe,
            self.lookback_candles,
        )
        run_immediately = True
        while True:
            try:
                control_status, _ = self._get_effective_control_status()
                if control_status in VALID_SLEEP_STATUSES:
                    self._safe_write_heartbeat(
                        "SLEEPING", f"sleep_by_scheduler:{control_status}"
                    )
                    time.sleep(self.scheduler_sleep_check_sec)
                    continue

                if run_immediately:
                    run_immediately = False
                else:
                    self._sleep_until_next_boundary()
                self._safe_write_heartbeat("RUNNING", "loop_started")
                self.run_once()
                self._safe_write_heartbeat("RUNNING", "loop_ok")
            except Exception as exc:
                self.logger.exception("15m pair-state loop failed")
                self._safe_write_heartbeat(
                    "ERROR", f"loop_error:{type(exc).__name__}"[:64]
                )
                time.sleep(10)
            finally:
                force_gc()

    def run_once(self) -> None:
        pairs = fetch_all(self.get_pairs_sql, self.mysql_api_file)
        if not pairs:
            self.logger.info("No enabled 15m pairs")
            return

        now_utc = datetime.utcnow()
        missing = [
            {"uuid": row["uuid"], "last_update_ts": now_utc}
            for row in pairs
            if row.get("pair_state_id") is None
        ]
        if missing:
            execute_many(
                self.insert_pair_state_sql,
                self.mysql_api_file,
                missing,
            )

        symbol_cache = self._build_hourly_symbol_cache(pairs)
        trade_rows = fetch_all(self.get_trade_results_sql, self.mysql_api_file)
        trade_map: dict[str, list[dict[str, Any]]] = {}
        for row in trade_rows:
            trade_map.setdefault(row["uuid"], []).append(row)

        updates: list[dict[str, Any]] = []
        failed = 0
        for pair in pairs:
            try:
                metrics = super()._build_pair_state_metrics(
                    pair_row=pair,
                    symbol_cache=symbol_cache,
                    raw_trade_rows=trade_map.get(pair["uuid"], []),
                    now_utc=now_utc,
                )
                metrics.pop("_mark_quarantine", None)
                metrics.pop("_remove_from_working_layer", None)
                self._apply_hourly_regime(metrics)
                updates.append(metrics)
            except Exception:
                failed += 1
                self.logger.exception(
                    "Failed 15m pair-state calculation | uuid=%s", pair["uuid"]
                )

        if updates:
            execute_many(
                self.update_pair_state_sql,
                self.mysql_api_file,
                updates,
            )
        self.logger.info(
            "15m pair-state complete | pairs=%s updated=%s failed=%s",
            len(pairs),
            len(updates),
            failed,
        )

    def _apply_hourly_regime(self, metrics: dict[str, Any]) -> None:
        if metrics.get("quarantine_reason"):
            metrics["level_30"] = "quarantine"
            metrics["level_180"] = "quarantine"
            return

        beta = metrics.get("beta")
        cointegrated = (
            metrics.get("adf") is not None
            and metrics.get("p_value") is not None
            and beta is not None
            and float(metrics["adf"]) < self.adf_threshold
            and float(metrics["p_value"]) < self.p_value_threshold
            and float(beta) > self.beta_raw_min
        )
        metrics_30 = {
            "num_trades": metrics["num_trades_30"],
            "expect": metrics["expect_30"],
        }
        metrics_180 = {
            "num_trades": metrics["num_trades_180"],
            "expect": metrics["expect_180"],
        }
        metrics["level_30"] = self._resolve_level_30(
            cointegrated, metrics_30
        )
        metrics["level_180"] = self._resolve_level_180(
            cointegrated, metrics_30, metrics_180
        )

    def _build_hourly_symbol_cache(
        self,
        pairs: list[dict[str, Any]],
    ) -> dict[str, pd.DataFrame]:
        symbols = sorted(
            {row["asset_1"] for row in pairs}
            | {row["asset_2"] for row in pairs}
        )
        cache: dict[str, pd.DataFrame] = {}
        with ThreadPoolExecutor(max_workers=self.symbol_refresh_workers) as pool:
            futures = {
                pool.submit(self._fetch_hourly_symbol, symbol): symbol
                for symbol in symbols
            }
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    cache[symbol] = future.result()
                except Exception:
                    self.logger.exception(
                        "Failed hourly data refresh | symbol=%s", symbol
                    )
        return cache

    def _fetch_hourly_symbol(self, symbol: str) -> pd.DataFrame:
        rows = fetch_ohlcv_with_retry(
            bybit_client=self.bybit,
            symbol=symbol,
            timeframe=self.timeframe,
            limit=self.lookback_candles + self.fetch_buffer_candles,
        )
        frame = pd.DataFrame(
            rows, columns=["ts", "open", "high", "low", "close", "volume"]
        )
        frame = self._normalize_ohlcv_frame(frame)
        frame = self._keep_closed_candles(frame, 60)
        frame = frame.tail(self.lookback_candles).reset_index(drop=True)
        if len(frame) < self.lookback_candles:
            raise ValueError(
                f"Hourly rows below lookback for {symbol}: "
                f"{len(frame)} < {self.lookback_candles}"
            )
        write_symbol_ohlcv_parquet_to_storage(
            symbol, frame, self.parquet_storage_name
        )
        return read_symbol_ohlcv_parquet_from_storage(
            symbol, self.parquet_storage_name
        )

    def _sleep_until_next_boundary(self) -> None:
        now = datetime.now(timezone.utc)
        current = now.replace(minute=0, second=0, microsecond=0)
        next_boundary = current + timedelta(hours=1)
        seconds = max(
            0.0,
            (next_boundary - now).total_seconds() + self.loop_start_delay_sec,
        )
        self._safe_write_heartbeat("SLEEPING", "waiting_next_1h_close")
        time.sleep(seconds)

    @staticmethod
    def _keep_closed_candles(
        frame: pd.DataFrame,
        timeframe_minutes: int,
    ) -> pd.DataFrame:
        now = datetime.now(timezone.utc)
        minute = (now.minute // timeframe_minutes) * timeframe_minutes
        boundary = now.replace(minute=minute, second=0, microsecond=0)
        boundary_ms = int(boundary.timestamp() * 1000)
        return (
            frame[frame["ts"] < boundary_ms]
            .sort_values("ts")
            .drop_duplicates("ts", keep="last")
            .reset_index(drop=True)
        )


if __name__ == "__main__":
    PairStateWorker15Min().run_forever()
