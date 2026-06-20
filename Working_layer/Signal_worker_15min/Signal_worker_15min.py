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
from Common.exchange.bybit_client import create_bybit_client, fetch_ohlcv_with_retry
from Common.parquet.parquet_reader import read_symbol_ohlcv_parquet_from_storage
from Common.parquet.parquet_writer import write_symbol_ohlcv_parquet_to_storage
from Common.statistics.beta_calc import build_spread_from_beta
from Common.statistics.zscore import calculate_zscore_summary
from Common.utils.cleanup import force_gc
from Common.utils.logger import setup_logger
from Common.utils.sql_file_loader import load_sql_file
from Working_layer.Signal_worker.Signal_worker import SignalWorker


VALID_SLEEP_STATUSES = {"SLEEP", "STOP", "SL_BLOCK"}


class SignalWorker15Min(SignalWorker):
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
            / "Signal_worker_15min"
            / "signal_worker_15min.log"
        )
        self.logger = setup_logger(
            logger_name="WorkingLayer.SignalWorker15Min",
            log_file_path=self.log_path,
        )
        self.rules = self._load_rules_file(self.rules_path)

        self.mysql_api_file = self.rules.get("mysql_api_file", "api_mysql_main.txt")
        self.bybit_api_file = self.rules.get("bybit_api_file", "api_bybit_main.txt")
        self.worker_name = self.rules.get("worker_name", "signal_worker_15min")
        self.timeframe = self.rules.get("timeframe", "15m")
        self.timeframe_minutes = int(
            self.rules.get("timeframe_minutes", "15")
        )
        self.loop_start_delay_sec = int(
            self.rules.get("loop_start_delay_sec", "5")
        )
        self.scheduler_sleep_check_sec = int(
            self.rules.get("scheduler_sleep_check_sec", "30")
        )
        self.max_lookback_candles = int(
            self.rules.get("max_lookback_candles", "300")
        )
        self.fetch_buffer_candles = int(
            self.rules.get("fetch_buffer_candles", "5")
        )
        self.parquet_storage_name = self.rules.get(
            "parquet_storage_name", "bybit_linear_15m"
        )
        self.symbol_refresh_workers = int(
            self.rules.get("symbol_refresh_workers", "8")
        )
        self.adf_threshold = float(self.rules.get("adf_threshold", "-2.5"))
        self.p_value_threshold = float(
            self.rules.get("p_value_threshold", "0.10")
        )
        self.beta_min = float(
            self.rules.get("beta_min", self.rules.get("beta_raw_min", "0.10"))
        )
        self.beta_signal_min = float(
            self.rules.get("beta_signal_min", "0.7")
        )
        self.beta_signal_max = float(
            self.rules.get("beta_signal_max", "1.3")
        )

        self.get_active_pairs_sql = load_sql_file(
            self.sql_dir / "get_active_pairs.txt"
        )
        self.update_signal_metrics_sql = load_sql_file(
            self.sql_dir / "update_signal_metrics.txt"
        )
        self.get_scheduler_statuses_sql = load_sql_file(
            self.sql_dir / "get_scheduler_statuses.txt"
        )
        # Signal OHLCV is public market data; avoid private demo endpoints.
        self.bybit = create_bybit_client("")

    def run_forever(self) -> None:
        self.logger.info("15m signal worker started")
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
                    self._sleep_until_next_candle_boundary()
                self._safe_write_heartbeat("RUNNING", "loop_started")
                self.run_once()
                self._safe_write_heartbeat("RUNNING", "loop_ok")
            except Exception as exc:
                self.logger.exception("15m signal loop failed")
                self._safe_write_heartbeat(
                    "ERROR", f"loop_error:{type(exc).__name__}"[:64]
                )
                time.sleep(10)
            finally:
                force_gc()

    def run_once(self) -> None:
        pairs = fetch_all(self.get_active_pairs_sql, self.mysql_api_file)
        if not pairs:
            self.logger.info("No enabled 15m pairs")
            return

        cache = self._build_symbol_cache_15m(pairs)
        updates: list[dict[str, Any]] = []
        failed = 0
        for pair in pairs:
            try:
                updates.append(self._process_pair_15m(pair, cache))
            except Exception:
                failed += 1
                self.logger.exception(
                    "Failed 15m signal calculation | uuid=%s", pair["uuid"]
                )
        if updates:
            execute_many(
                self.update_signal_metrics_sql,
                self.mysql_api_file,
                updates,
            )
        self.logger.info(
            "15m signal complete | pairs=%s updated=%s failed=%s",
            len(pairs),
            len(updates),
            failed,
        )

    def _process_pair_15m(
        self,
        pair: dict[str, Any],
        cache: dict[str, pd.DataFrame],
    ) -> dict[str, Any]:
        asset_1 = pair["asset_1"]
        asset_2 = pair["asset_2"]
        rolling_window = int(pair["rolling_window_15m"])
        if rolling_window <= 1 or rolling_window > self.max_lookback_candles:
            raise ValueError(
                f"Invalid rolling_window_15m={rolling_window} "
                f"for uuid={pair['uuid']}"
            )
        if asset_1 not in cache or asset_2 not in cache:
            raise ValueError(f"Missing 15m market data for uuid={pair['uuid']}")

        merged = self._merge_pair_frames(
            cache[asset_1], cache[asset_2], asset_1, asset_2
        )
        if len(merged) < rolling_window:
            raise ValueError(
                f"Aligned 15m rows below window for uuid={pair['uuid']}"
            )
        merged = merged.tail(rolling_window).reset_index(drop=True)
        beta = float(pair["beta"])
        beta_for_signal = max(
            self.beta_signal_min,
            min(self.beta_signal_max, beta),
        )
        spread = build_spread_from_beta(
            price_1=merged[f"{asset_1}_close"],
            price_2=merged[f"{asset_2}_close"],
            beta=beta_for_signal,
            use_log=True,
        )
        z_summary = calculate_zscore_summary(spread)
        asset1_15m, asset1_1h = self._extract_liquidity_15m(
            merged[f"{asset_1}_volume"]
        )
        asset2_15m, asset2_1h = self._extract_liquidity_15m(
            merged[f"{asset_2}_volume"]
        )
        this_month, prev_month, signal_hit = self._resolve_pair_signal_counts(
            pair=pair,
            current_last_z_score=z_summary["last_z_score"],
        )
        self.logger.info(
            "15m pair processed | uuid=%s z=%.4f signal_hit=%s",
            pair["uuid"],
            z_summary["last_z_score"],
            signal_hit,
        )
        return {
            "uuid": pair["uuid"],
            "last_update_ts": datetime.utcnow(),
            "last_z_score": z_summary["last_z_score"],
            "max_z_score": z_summary["max_z_score"],
            "min_z_score": z_summary["min_z_score"],
            # Compatibility columns: these contain latest 15m and trailing 1h volume.
            "asset1_5m_vol": asset1_15m,
            "asset1_1h_vol": asset1_1h,
            "asset2_5m_vol": asset2_15m,
            "asset2_1h_vol": asset2_1h,
            "signal_this_month": this_month,
            "signal_prev_month": prev_month,
        }

    def _resolve_pair_signal_counts(
        self,
        pair: dict[str, Any],
        current_last_z_score: float,
    ) -> tuple[int, int, int]:
        signal_this_month = int(pair.get("signal_this_month") or 0)
        signal_prev_month = int(pair.get("signal_prev_month") or 0)
        stored_dt = self._coerce_datetime(pair.get("signal_last_update_ts"))
        now = datetime.utcnow()
        if stored_dt is not None and (
            stored_dt.year != now.year or stored_dt.month != now.month
        ):
            signal_prev_month = signal_this_month
            signal_this_month = 0

        threshold = float(pair["entry_z_threshold"])
        previous = pair.get("prev_last_z_score")
        previous_abs = abs(float(previous)) if previous is not None else 0.0
        current_abs = abs(float(current_last_z_score))
        signal_hit = int(
            self._pair_is_countable_signal_source(pair)
            and previous_abs < threshold <= current_abs
        )
        signal_this_month += signal_hit
        return signal_this_month, signal_prev_month, signal_hit

    def _pair_is_countable_signal_source(self, pair: dict[str, Any]) -> bool:
        required = ("adf", "p_value", "beta")
        if any(pair.get(key) is None for key in required):
            return False
        if float(pair["adf"]) >= self.adf_threshold:
            return False
        if float(pair["p_value"]) >= self.p_value_threshold:
            return False
        if float(pair["beta"]) <= self.beta_min:
            return False
        if str(pair.get("level_30") or "").lower() == "quarantine":
            return False
        if str(pair.get("level_180") or "").lower() == "quarantine":
            return False
        quarantine_until = self._coerce_datetime(pair.get("quarantine_until"))
        return quarantine_until is None or quarantine_until <= datetime.utcnow()

    def _build_symbol_cache_15m(
        self,
        pairs: list[dict[str, Any]],
    ) -> dict[str, pd.DataFrame]:
        symbols = sorted(
            {row["asset_1"] for row in pairs}
            | {row["asset_2"] for row in pairs}
        )
        cache: dict[str, pd.DataFrame] = {}
        markets = self.bybit.load_markets()
        missing_markets = [symbol for symbol in symbols if symbol not in markets]
        if missing_markets:
            self.logger.error(
                "Symbols missing from Bybit markets | count=%s | symbols=%s",
                len(missing_markets),
                missing_markets,
            )

        with ThreadPoolExecutor(max_workers=self.symbol_refresh_workers) as pool:
            futures = {
                pool.submit(self._fetch_symbol_15m, symbol): symbol
                for symbol in symbols
                if symbol in markets
            }
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    cache[symbol] = future.result()
                except Exception:
                    self.logger.exception(
                        "Failed 15m data refresh | symbol=%s", symbol
                    )
        return cache

    def _fetch_symbol_15m(self, symbol: str) -> pd.DataFrame:
        rows = fetch_ohlcv_with_retry(
            bybit_client=self.bybit,
            symbol=symbol,
            timeframe=self.timeframe,
            limit=self.max_lookback_candles + self.fetch_buffer_candles,
        )
        frame = pd.DataFrame(
            rows, columns=["ts", "open", "high", "low", "close", "volume"]
        )
        frame = self._normalize_ohlcv_frame(frame)
        frame = self._keep_closed_candles(frame)
        frame = frame.tail(self.max_lookback_candles).reset_index(drop=True)
        if len(frame) < self.max_lookback_candles:
            raise ValueError(
                f"15m rows below maximum lookback for {symbol}: "
                f"{len(frame)} < {self.max_lookback_candles}"
            )
        write_symbol_ohlcv_parquet_to_storage(
            symbol, frame, self.parquet_storage_name
        )
        return read_symbol_ohlcv_parquet_from_storage(
            symbol, self.parquet_storage_name
        )

    def _keep_closed_candles(self, frame: pd.DataFrame) -> pd.DataFrame:
        now = datetime.now(timezone.utc)
        minute = (now.minute // self.timeframe_minutes) * self.timeframe_minutes
        boundary = now.replace(minute=minute, second=0, microsecond=0)
        boundary_ms = int(boundary.timestamp() * 1000)
        return (
            frame[frame["ts"] < boundary_ms]
            .sort_values("ts")
            .drop_duplicates("ts", keep="last")
            .reset_index(drop=True)
        )

    def _sleep_until_next_candle_boundary(self) -> None:
        now = datetime.now(timezone.utc)
        minute = (now.minute // self.timeframe_minutes) * self.timeframe_minutes
        current = now.replace(minute=minute, second=0, microsecond=0)
        next_boundary = current + timedelta(minutes=self.timeframe_minutes)
        seconds = max(
            0.0,
            (next_boundary - now).total_seconds() + self.loop_start_delay_sec,
        )
        self._safe_write_heartbeat("SLEEPING", "waiting_next_15m_close")
        time.sleep(seconds)

    @staticmethod
    def _extract_liquidity_15m(series: pd.Series) -> tuple[float, float]:
        clean = pd.to_numeric(series, errors="coerce").dropna().astype(float)
        if clean.empty:
            return 0.0, 0.0
        return float(clean.iloc[-1]), float(clean.tail(4).sum())


if __name__ == "__main__":
    SignalWorker15Min().run_forever()
