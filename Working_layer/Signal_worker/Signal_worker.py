from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from Common.db.db_execute import fetch_all, execute_many
from Common.db.heartbeat_writer import write_heartbeat
from Common.exchange.bybit_client import create_bybit_client, fetch_ohlcv_with_retry
from Common.parquet.parquet_reader import parquet_exists, read_symbol_ohlcv_parquet
from Common.parquet.parquet_updater import merge_symbol_ohlcv_parquet, replace_symbol_ohlcv_parquet
from Common.statistics.zscore import build_spread_series, calculate_zscore_summary
from Common.utils.cleanup import force_gc
from Common.utils.logger import setup_logger
from Common.utils.sql_file_loader import load_sql_file


VALID_SLEEP_STATUSES = {"SLEEP", "STOP", "SL_BLOCK"}


class SignalWorker:
    def __init__(self) -> None:
        self.worker_dir = Path(__file__).resolve().parent
        self.project_root = self.worker_dir.parents[1]

        self.rules_path = self.worker_dir / "rules" / "rules.txt"
        self.sql_dir = self.worker_dir / "sql_queries"
        self.log_path = self.project_root / "data" / "logs" / "Working_layer" / "Signal_worker" / "signal_worker.log"

        self.logger = setup_logger(
            logger_name="WorkingLayer.SignalWorker",
            log_file_path=self.log_path,
        )

        self.rules = self._load_rules_file(self.rules_path)

        self.timezone_name = self.rules.get("timezone", "UTC")
        self.mysql_api_file = self.rules.get("mysql_api_file", "api_mysql_main.txt")
        self.bybit_api_file = self.rules.get("bybit_api_file", "api_bybit_main.txt")
        self.worker_name = self.rules.get("worker_name", "signal_worker")

        self.entry_abs_z_threshold = float(self.rules.get("entry_abs_z_threshold", "2.0"))
        self.adf_threshold = float(self.rules.get("adf_threshold", "-2.9"))
        self.p_value_threshold = float(self.rules.get("p_value_threshold", "0.05"))

        self.lookback_candles = int(self.rules.get("lookback_candles", "1000"))
        self.timeframe = self.rules.get("timeframe", "5m")
        self.loop_interval_minutes = int(self.rules.get("loop_interval_minutes", "5"))
        self.loop_start_delay_sec = int(self.rules.get("loop_start_delay_sec", "3"))
        self.full_refresh_if_rows_below = int(self.rules.get("full_refresh_if_rows_below", "1000"))
        self.incremental_fetch_limit = int(self.rules.get("incremental_fetch_limit", "20"))
        self.scheduler_sleep_check_sec = int(self.rules.get("scheduler_sleep_check_sec", "30"))
        self.symbol_refresh_workers = int(self.rules.get("symbol_refresh_workers", "8"))

        self.get_active_pairs_sql = load_sql_file(self.sql_dir / "get_active_pairs.txt")
        self.get_scheduler_statuses_sql = load_sql_file(self.sql_dir / "get_scheduler_statuses.txt")
        self.update_signal_metrics_sql = load_sql_file(self.sql_dir / "update_signal_metrics.txt")

        self.bybit = create_bybit_client(self.bybit_api_file)

    def run_forever(self) -> None:
        self.logger.info(
            "Signal worker started | timeframe=%s | lookback=%s | threshold=%.3f | symbol_workers=%s",
            self.timeframe,
            self.lookback_candles,
            self.entry_abs_z_threshold,
            self.symbol_refresh_workers,
        )

        while True:
            try:
                control_status, control_comment = self._get_effective_control_status()

                if control_status in VALID_SLEEP_STATUSES:
                    self._safe_write_heartbeat(
                        runtime_status="SLEEPING",
                        comment=self._truncate_comment(f"sleep_by_scheduler:{control_status}"),
                    )
                    self.logger.info(
                        "Signal worker sleeping by scheduler | status=%s | comment=%s",
                        control_status,
                        control_comment,
                    )
                    time.sleep(self.scheduler_sleep_check_sec)
                    continue

                self._sleep_until_next_candle_boundary()

                loop_started = time.time()
                self._safe_write_heartbeat("RUNNING", "loop_started")

                self.run_once()

                self._safe_write_heartbeat("RUNNING", "loop_ok")

                elapsed = time.time() - loop_started
                self.logger.info("Signal worker loop finished | elapsed_sec=%.2f", elapsed)

            except Exception as exc:
                self.logger.exception("Signal worker loop failed")
                self._safe_write_heartbeat(
                    runtime_status="ERROR",
                    comment=self._truncate_comment(f"loop_error:{type(exc).__name__}"),
                )
                time.sleep(10)

            finally:
                force_gc()

    def run_once(self) -> None:
        pair_rows = self._fetch_active_pairs()
        self.logger.info("Fetched pairs from signal_table | count=%s", len(pair_rows))

        if not pair_rows:
            return

        symbol_cache = self._build_symbol_cache(pair_rows)

        update_params_list: list[dict[str, Any]] = []
        signal_hits = 0
        failed_pairs = 0

        for pair_row in pair_rows:
            uuid = pair_row["uuid"]

            try:
                metrics = self._process_pair(pair_row, symbol_cache)
                signal_hits += metrics.pop("_signal_hit", 0)
                update_params_list.append(metrics)

            except Exception:
                failed_pairs += 1
                self.logger.exception("Failed processing pair | uuid=%s", uuid)

        updated_rows = 0
        if update_params_list:
            updated_rows = execute_many(
                sql=self.update_signal_metrics_sql,
                api_file_name=self.mysql_api_file,
                params_seq=update_params_list,
            )

        self.logger.info(
            "Signal worker DB update complete | pairs_total=%s | updated_rows=%s | signal_hits=%s | failed_pairs=%s",
            len(pair_rows),
            updated_rows,
            signal_hits,
            failed_pairs,
        )

    def _build_symbol_cache(self, pair_rows: list[dict[str, Any]]) -> dict[str, pd.DataFrame]:
        unique_symbols = sorted(
            {row["asset_1"] for row in pair_rows}.union({row["asset_2"] for row in pair_rows})
        )

        self.logger.info("Building symbol cache | unique_symbols=%s", len(unique_symbols))

        symbol_cache: dict[str, pd.DataFrame] = {}
        failed_symbols: list[str] = []

        with ThreadPoolExecutor(max_workers=self.symbol_refresh_workers) as executor:
            future_map = {
                executor.submit(self._ensure_symbol_dataset, symbol): symbol
                for symbol in unique_symbols
            }

            for future in as_completed(future_map):
                symbol = future_map[future]
                try:
                    symbol_cache[symbol] = future.result()
                except Exception:
                    failed_symbols.append(symbol)
                    self.logger.exception("Failed building symbol dataset | symbol=%s", symbol)

        if failed_symbols:
            self.logger.warning(
                "Some symbols failed during cache build | failed_count=%s | examples=%s",
                len(failed_symbols),
                failed_symbols[:10],
            )

        self.logger.info(
            "Symbol cache ready | loaded=%s | failed=%s",
            len(symbol_cache),
            len(failed_symbols),
        )

        return symbol_cache

    def _process_pair(
        self,
        pair_row: dict[str, Any],
        symbol_cache: dict[str, pd.DataFrame],
    ) -> dict[str, Any]:
        uuid = pair_row["uuid"]
        asset_1 = pair_row["asset_1"]
        asset_2 = pair_row["asset_2"]

        if asset_1 not in symbol_cache:
            raise ValueError(f"Missing cached OHLCV for asset_1={asset_1}")
        if asset_2 not in symbol_cache:
            raise ValueError(f"Missing cached OHLCV for asset_2={asset_2}")

        beta_norm = pair_row.get("beta_norm")
        prev_last_z_score = pair_row.get("prev_last_z_score")
        signal_this_month = int(pair_row.get("signal_this_month") or 0)
        signal_prev_month = int(pair_row.get("signal_prev_month") or 0)
        signal_last_update_ts = pair_row.get("signal_last_update_ts")

        df_1 = symbol_cache[asset_1]
        df_2 = symbol_cache[asset_2]

        merged = self._merge_pair_frames(df_1, df_2, asset_1, asset_2)
        if len(merged) < self.lookback_candles:
            raise ValueError(
                f"Not enough aligned candles for uuid={uuid}. "
                f"got={len(merged)} need={self.lookback_candles}"
            )

        merged = merged.tail(self.lookback_candles).reset_index(drop=True)

        spread = build_spread_series(
            asset_1_close=merged[f"{asset_1}_close"],
            asset_2_close=merged[f"{asset_2}_close"],
            beta_norm=beta_norm,
        )
        z_summary = calculate_zscore_summary(spread)

        asset1_5m_vol, asset1_1h_vol = self._extract_liquidity(merged[f"{asset_1}_volume"])
        asset2_5m_vol, asset2_1h_vol = self._extract_liquidity(merged[f"{asset_2}_volume"])

        countable_signal_source = self._pair_is_countable_signal_source(pair_row)

        new_signal_this_month, new_signal_prev_month, signal_hit = self._resolve_monthly_signal_counts(
            prev_last_z_score=prev_last_z_score,
            current_last_z_score=z_summary["last_z_score"],
            signal_this_month=signal_this_month,
            signal_prev_month=signal_prev_month,
            signal_last_update_ts=signal_last_update_ts,
            allow_signal_count=countable_signal_source,
        )

        self.logger.info(
            "Pair processed | uuid=%s | last_z=%.4f | max_z=%.4f | min_z=%.4f | signal_hit=%s | countable=%s",
            uuid,
            z_summary["last_z_score"],
            z_summary["max_z_score"],
            z_summary["min_z_score"],
            signal_hit,
            countable_signal_source,
        )

        return {
            "uuid": uuid,
            "last_update_ts": datetime.utcnow(),
            "last_z_score": z_summary["last_z_score"],
            "max_z_score": z_summary["max_z_score"],
            "min_z_score": z_summary["min_z_score"],
            "asset1_5m_vol": asset1_5m_vol,
            "asset1_1h_vol": asset1_1h_vol,
            "asset2_5m_vol": asset2_5m_vol,
            "asset2_1h_vol": asset2_1h_vol,
            "signal_this_month": new_signal_this_month,
            "signal_prev_month": new_signal_prev_month,
            "_signal_hit": signal_hit,
        }

    def _keep_closed_candles_only(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove still-open current candle if present.

        Rule:
        keep rows with ts strictly less than last closed 5m boundary.
        """
        if df.empty:
            return df

        last_closed_boundary = self._get_last_closed_5m_boundary(datetime.now(timezone.utc))
        last_closed_ts_ms = int(last_closed_boundary.timestamp() * 1000)

        closed_df = df[df["ts"] < last_closed_ts_ms].copy()
        closed_df = closed_df.sort_values("ts").drop_duplicates(subset=["ts"], keep="last").reset_index(drop=True)
        return closed_df


    def _ensure_symbol_dataset(self, symbol: str) -> pd.DataFrame:
        """
        Full refresh each cycle:
        - fetch latest candles from Bybit
        - keep only CLOSED candles
        - sort by ts
        - drop duplicates
        - keep last lookback_candles rows
        - overwrite parquet
        """
        fetch_limit = self.lookback_candles + 5

        rows = fetch_ohlcv_with_retry(
            bybit_client=self.bybit,
            symbol=symbol,
            timeframe=self.timeframe,
            limit=fetch_limit,
        )

        fresh_df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        fresh_df = self._normalize_ohlcv_frame(fresh_df)
        fresh_df = self._keep_closed_candles_only(fresh_df)
        fresh_df = fresh_df.tail(self.lookback_candles).reset_index(drop=True)

        if len(fresh_df) < self.lookback_candles:
            raise ValueError(
                f"Fetched rows below required lookback for symbol={symbol}. "
                f"got={len(fresh_df)}, required={self.lookback_candles}"
            )

        replace_symbol_ohlcv_parquet(symbol, fresh_df.values.tolist())

        df = read_symbol_ohlcv_parquet(symbol).copy()
        df = self._normalize_ohlcv_frame(df)
        df = self._keep_closed_candles_only(df)
        df = df.tail(self.lookback_candles).reset_index(drop=True)

        return df

    def _fetch_active_pairs(self) -> list[dict[str, Any]]:
        return fetch_all(
            sql=self.get_active_pairs_sql,
            api_file_name=self.mysql_api_file,
        )

    def _get_effective_control_status(self) -> tuple[str, str]:
        rows = fetch_all(
            sql=self.get_scheduler_statuses_sql,
            api_file_name=self.mysql_api_file,
            params={"worker_id": self.worker_name},
        )

        mapped = {row["worker_id"]: row for row in rows}
        global_row = mapped.get("GLOBAL")
        worker_row = mapped.get(self.worker_name)

        if global_row and str(global_row.get("control_status", "")).upper() in VALID_SLEEP_STATUSES:
            return str(global_row["control_status"]).upper(), str(global_row.get("comment") or "")

        if worker_row:
            return str(worker_row.get("control_status", "RUNNING")).upper(), str(worker_row.get("comment") or "")

        return "RUNNING", "default_missing_scheduler_row"

    def _safe_write_heartbeat(self, runtime_status: str, comment: str | None) -> None:
        try:
            write_heartbeat(
                worker_id=self.worker_name,
                runtime_status=runtime_status,
                comment=comment,
                api_file_name=self.mysql_api_file,
            )
        except Exception:
            self.logger.exception(
                "Failed to write heartbeat | worker=%s | runtime_status=%s | comment=%s",
                self.worker_name,
                runtime_status,
                comment,
            )

    def _sleep_until_next_candle_boundary(self) -> None:
        now = datetime.now(timezone.utc)
        next_boundary = self._get_next_5m_boundary(now)
        sleep_seconds = max(0.0, (next_boundary - now).total_seconds() + self.loop_start_delay_sec)

        self.logger.info(
            "Sleeping until next candle boundary | now=%s | next=%s | sleep_sec=%.2f",
            now.strftime("%Y-%m-%d %H:%M:%S"),
            next_boundary.strftime("%Y-%m-%d %H:%M:%S"),
            sleep_seconds,
        )
        self._safe_write_heartbeat("SLEEPING", "waiting_next_5m_close")
        time.sleep(sleep_seconds)

    def _needs_incremental_refresh(self, df: pd.DataFrame) -> bool:
        if df.empty:
            return True

        last_ts_ms = int(df["ts"].max())
        last_candle_dt = datetime.fromtimestamp(last_ts_ms / 1000, tz=timezone.utc)
        expected_last_closed = self._get_last_closed_5m_boundary(datetime.now(timezone.utc))

        return last_candle_dt < expected_last_closed

    @staticmethod
    def _get_next_5m_boundary(now_utc: datetime) -> datetime:
        minute_floor = (now_utc.minute // 5) * 5
        current_boundary = now_utc.replace(minute=minute_floor, second=0, microsecond=0)
        return current_boundary + timedelta(minutes=5)

    @staticmethod
    def _get_last_closed_5m_boundary(now_utc: datetime) -> datetime:
        minute_floor = (now_utc.minute // 5) * 5
        return now_utc.replace(minute=minute_floor, second=0, microsecond=0)

    @staticmethod
    def _normalize_ohlcv_frame(df: pd.DataFrame) -> pd.DataFrame:
        required_cols = {"ts", "open", "high", "low", "close", "volume"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"OHLCV parquet is missing columns: {sorted(missing)}")

        normalized = df.copy()
        normalized["ts"] = pd.to_numeric(normalized["ts"], errors="coerce")
        normalized["close"] = pd.to_numeric(normalized["close"], errors="coerce")
        normalized["volume"] = pd.to_numeric(normalized["volume"], errors="coerce")

        normalized = normalized.dropna(subset=["ts", "close", "volume"])
        normalized = normalized.drop_duplicates(subset=["ts"], keep="last")
        normalized = normalized.sort_values("ts").reset_index(drop=True)

        return normalized

    @staticmethod
    def _merge_pair_frames(
        df_1: pd.DataFrame,
        df_2: pd.DataFrame,
        asset_1: str,
        asset_2: str,
    ) -> pd.DataFrame:
        a1 = df_1[["ts", "close", "volume"]].rename(
            columns={
                "close": f"{asset_1}_close",
                "volume": f"{asset_1}_volume",
            }
        )
        a2 = df_2[["ts", "close", "volume"]].rename(
            columns={
                "close": f"{asset_2}_close",
                "volume": f"{asset_2}_volume",
            }
        )

        merged = pd.merge(a1, a2, on="ts", how="inner")
        merged = merged.dropna().sort_values("ts").reset_index(drop=True)
        return merged

    @staticmethod
    def _extract_liquidity(volume_series: pd.Series) -> tuple[float, float]:
        clean = pd.to_numeric(volume_series, errors="coerce").dropna().astype(float)
        if clean.empty:
            return 0.0, 0.0

        last_5m = float(clean.iloc[-1])
        last_1h = float(clean.tail(12).sum())
        return last_5m, last_1h

    def _pair_is_countable_signal_source(self, pair_row: dict[str, Any]) -> bool:
        adf = pair_row.get("adf")
        p_value = pair_row.get("p_value")
        level_30 = pair_row.get("level_30")
        level_180 = pair_row.get("level_180")
        quarantine_until = pair_row.get("quarantine_until")

        if adf is None or p_value is None:
            return False

        if float(adf) >= self.adf_threshold:
            return False

        if float(p_value) >= self.p_value_threshold:
            return False

        if str(level_30 or "").lower() == "quarantine":
            return False

        if str(level_180 or "").lower() == "quarantine":
            return False

        q_until = self._coerce_datetime(quarantine_until)
        if q_until is not None and q_until > datetime.utcnow():
            return False

        return True

    def _resolve_monthly_signal_counts(
        self,
        prev_last_z_score: float | None,
        current_last_z_score: float,
        signal_this_month: int,
        signal_prev_month: int,
        signal_last_update_ts: Any,
        allow_signal_count: bool,
    ) -> tuple[int, int, int]:
        current_month = datetime.utcnow().month
        current_year = datetime.utcnow().year

        stored_dt = self._coerce_datetime(signal_last_update_ts)

        if stored_dt is not None:
            if stored_dt.year != current_year or stored_dt.month != current_month:
                signal_prev_month = signal_this_month
                signal_this_month = 0

        prev_abs = abs(float(prev_last_z_score)) if prev_last_z_score is not None else 0.0
        current_abs = abs(float(current_last_z_score))

        signal_hit = 1 if (
            allow_signal_count
            and prev_abs < self.entry_abs_z_threshold
            and current_abs >= self.entry_abs_z_threshold
        ) else 0

        if signal_hit == 1:
            signal_this_month += 1

        return signal_this_month, signal_prev_month, signal_hit

    @staticmethod
    def _coerce_datetime(value: Any) -> datetime | None:
        if value is None:
            return None

        if isinstance(value, datetime):
            return value

        try:
            return pd.to_datetime(value).to_pydatetime()
        except Exception:
            return None

    @staticmethod
    def _truncate_comment(comment: str | None, max_len: int = 64) -> str | None:
        if comment is None:
            return None
        return str(comment)[:max_len]

    @staticmethod
    def _load_rules_file(file_path: Path) -> dict[str, str]:
        if not file_path.exists():
            raise FileNotFoundError(f"Rules file not found: {file_path}")

        rules: dict[str, str] = {}

        for raw_line in file_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()

            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                raise ValueError(f"Invalid rules line (missing '='): {line}")

            key, value = line.split("=", 1)
            rules[key.strip()] = value.strip()

        if not rules:
            raise ValueError(f"Rules file is empty: {file_path}")

        return rules


if __name__ == "__main__":
    worker = SignalWorker()
    worker.run_forever()