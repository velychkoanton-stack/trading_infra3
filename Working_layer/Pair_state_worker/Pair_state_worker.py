from __future__ import annotations

import math
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from Common.db.db_execute import fetch_all, execute, execute_many
from Common.db.heartbeat_writer import write_heartbeat
from Common.exchange.bybit_client import build_not_in_params
from Common.parquet.parquet_reader import parquet_exists, read_symbol_ohlcv_parquet
from Common.statistics.adf_test import run_adf_test_from_series
from Common.statistics.beta_calc import (
    align_close_series,
    build_spread_from_beta,
    calculate_beta_ols,
    normalize_beta,
)
from Common.statistics.half_life import calculate_half_life
from Common.statistics.hurst import calculate_hurst_exponent
from Common.statistics.spread_stats import calculate_spread_skew, calculate_spread_kurt
from Common.utils.cleanup import force_gc
from Common.utils.logger import setup_logger
from Common.utils.sql_file_loader import load_sql_file


VALID_SLEEP_STATUSES = {"SLEEP", "STOP", "SL_BLOCK"}


class PairStateWorker:
    def __init__(self) -> None:
        self.worker_dir = Path(__file__).resolve().parent
        self.project_root = self.worker_dir.parents[1]

        self.rules_path = self.worker_dir / "rules" / "rules.txt"
        self.sql_dir = self.worker_dir / "sql_queries"
        self.log_path = self.project_root / "data" / "logs" / "Working_layer" / "Pair_state_worker" / "pair_state_worker.log"

        self.logger = setup_logger(
            logger_name="WorkingLayer.PairStateWorker",
            log_file_path=self.log_path,
        )

        self.rules = self._load_rules_file(self.rules_path)

        self.mysql_api_file = self.rules.get("mysql_api_file", "api_mysql_main.txt")
        self.worker_name = self.rules.get("worker_name", "pair_state_worker")
        self.loop_minutes = int(self.rules.get("loop_minutes", "15"))
        self.scheduler_sleep_check_sec = int(self.rules.get("scheduler_sleep_check_sec", "30"))
        self.lookback_candles = int(self.rules.get("lookback_candles", "1000"))
        self.adf_threshold = float(self.rules.get("adf_threshold", "-2.9"))
        self.p_value_threshold = float(self.rules.get("p_value_threshold", "0.05"))
        self.quarantine_days = int(self.rules.get("quarantine_days", "14"))
        self.quarantine_losing_days_streak = int(self.rules.get("quarantine_losing_days_streak", "5"))
        self.same_event_window_minutes = int(self.rules.get("same_event_window_minutes", "10"))
        self.level2_180_min_trades = int(self.rules.get("level2_180_min_trades", "20"))
        self.level2_30_min_trades = int(self.rules.get("level2_30_min_trades", "5"))
        self.removal_min_trades = int(self.rules.get("removal_min_trades", "25"))
        self.use_log_prices_for_beta = self._parse_bool(self.rules.get("use_log_prices_for_beta", "1"))
        self.use_log_prices_for_adf = self._parse_bool(self.rules.get("use_log_prices_for_adf", "1"))

        self.get_scheduler_statuses_sql = load_sql_file(self.sql_dir / "get_scheduler_statuses.txt")
        self.get_working_pairs_sql = load_sql_file(self.sql_dir / "get_working_pairs.txt")
        self.get_delisted_working_pairs_sql = load_sql_file(self.sql_dir / "get_delisted_working_pairs.txt")
        self.delete_signal_table_pair_sql = load_sql_file(self.sql_dir / "delete_signal_table_pair.txt")
        self.delete_pair_state_pair_sql = load_sql_file(self.sql_dir / "delete_pair_state_pair.txt")
        self.get_candidate_pairs_sql = load_sql_file(self.sql_dir / "get_candidate_pairs.txt")
        self.insert_signal_table_pair_sql = load_sql_file(self.sql_dir / "insert_signal_table_pair.txt")
        self.insert_pair_state_pair_sql = load_sql_file(self.sql_dir / "insert_pair_state_pair.txt")
        self.mark_pair_universe_in_main_table_sql = load_sql_file(self.sql_dir / "mark_pair_universe_in_main_table.txt")
        self.get_trade_res_for_pairs_sql_template = load_sql_file(self.sql_dir / "get_trade_res_for_pairs.txt")
        self.update_pair_state_metrics_sql = load_sql_file(self.sql_dir / "update_pair_state_metrics.txt")
        self.mark_pair_universe_quarantine_sql = load_sql_file(self.sql_dir / "mark_pair_universe_quarantine.txt")
        self.mark_pair_universe_removed_sql = load_sql_file(self.sql_dir / "mark_pair_universe_removed.txt")

    def run_forever(self) -> None:
        self.logger.info("Pair_state worker started | loop_minutes=%s", self.loop_minutes)

        while True:
            try:
                control_status, control_comment = self._get_effective_control_status()

                if control_status in VALID_SLEEP_STATUSES:
                    self._safe_write_heartbeat(
                        runtime_status="SLEEPING",
                        comment=self._truncate_comment(f"sleep_by_scheduler:{control_status}"),
                    )
                    self.logger.info(
                        "Pair_state worker sleeping by scheduler | status=%s | comment=%s",
                        control_status,
                        control_comment,
                    )
                    time.sleep(self.scheduler_sleep_check_sec)
                    continue

                loop_started = time.time()
                self._safe_write_heartbeat("RUNNING", "loop_started")

                self.run_once()

                self._safe_write_heartbeat("RUNNING", "loop_ok")
                elapsed = time.time() - loop_started
                sleep_seconds = max(1.0, self.loop_minutes * 60 - elapsed)

                self.logger.info("Pair_state worker loop finished | elapsed_sec=%.2f", elapsed)
                time.sleep(sleep_seconds)

            except Exception as exc:
                self.logger.exception("Pair_state worker loop failed")
                self._safe_write_heartbeat(
                    runtime_status="ERROR",
                    comment=self._truncate_comment(f"loop_error:{type(exc).__name__}"),
                )
                time.sleep(10)

            finally:
                force_gc()

    def run_once(self) -> None:
        self._remove_delisted_pairs()
        self._insert_new_candidate_pairs()

        working_pairs = self._fetch_working_pairs()
        if not working_pairs:
            self.logger.info("No working pairs found after sync")
            return

        pair_map = {row["uuid"]: row for row in working_pairs}
        symbol_cache = self._build_symbol_cache(working_pairs)
        trade_map = self._fetch_trade_map_for_pairs(list(pair_map.keys()))

        now_utc = datetime.utcnow()
        update_params_list: list[dict[str, Any]] = []

        quarantine_uuids: list[str] = []
        removed_uuids: list[str] = []

        for uuid, pair_row in pair_map.items():
            try:
                metrics = self._build_pair_state_metrics(
                    pair_row=pair_row,
                    symbol_cache=symbol_cache,
                    raw_trade_rows=trade_map.get(uuid, []),
                    now_utc=now_utc,
                )

                if metrics["_remove_from_working_layer"] == 1:
                    removed_uuids.append(uuid)
                    continue

                if metrics["_mark_quarantine"] == 1:
                    quarantine_uuids.append(uuid)

                metrics.pop("_remove_from_working_layer", None)
                metrics.pop("_mark_quarantine", None)
                update_params_list.append(metrics)

            except Exception:
                self.logger.exception("Failed building pair_state metrics | uuid=%s", uuid)

        if update_params_list:
            execute_many(
                sql=self.update_pair_state_metrics_sql,
                api_file_name=self.mysql_api_file,
                params_seq=update_params_list,
            )

        if quarantine_uuids:
            self._mark_pair_universe_quarantine(quarantine_uuids, now_utc)

        if removed_uuids:
            self._remove_bad_pairs_from_working_layer(removed_uuids, now_utc)

        self.logger.info(
            "Pair_state loop complete | working_pairs=%s | updated=%s | quarantined=%s | removed=%s",
            len(working_pairs),
            len(update_params_list),
            len(quarantine_uuids),
            len(removed_uuids),
        )

    def _remove_delisted_pairs(self) -> None:
        rows = fetch_all(
            sql=self.get_delisted_working_pairs_sql,
            api_file_name=self.mysql_api_file,
        )

        for row in rows:
            uuid = row["uuid"]
            execute(
                sql=self.delete_signal_table_pair_sql,
                api_file_name=self.mysql_api_file,
                params={"uuid": uuid},
            )
            execute(
                sql=self.delete_pair_state_pair_sql,
                api_file_name=self.mysql_api_file,
                params={"uuid": uuid},
            )

        if rows:
            self.logger.info("Removed delisted working pairs | count=%s", len(rows))

    def _insert_new_candidate_pairs(self) -> None:
        working_pairs = self._fetch_working_pairs()
        existing_uuids = {row["uuid"] for row in working_pairs}

        candidate_rows = fetch_all(
            sql=self.get_candidate_pairs_sql,
            api_file_name=self.mysql_api_file,
        )

        now_utc = datetime.utcnow()
        inserted = 0

        for row in candidate_rows:
            uuid = row["uuid"]
            if uuid in existing_uuids:
                continue

            execute(
                sql=self.insert_signal_table_pair_sql,
                api_file_name=self.mysql_api_file,
                params={
                    "uuid": uuid,
                    "asset_1": row["asset_1"],
                    "asset_2": row["asset_2"],
                    "tp": row["best_tp"],
                    "sl": row["best_sl"],
                    "last_update_ts": now_utc,
                },
            )
            execute(
                sql=self.insert_pair_state_pair_sql,
                api_file_name=self.mysql_api_file,
                params={
                    "uuid": uuid,
                    "last_update_ts": now_utc,
                },
            )
            execute(
                sql=self.mark_pair_universe_in_main_table_sql,
                api_file_name=self.mysql_api_file,
                params={
                    "uuid": uuid,
                    "dt_status_change": now_utc,
                },
            )
            inserted += 1

        if inserted:
            self.logger.info("Inserted new candidate pairs into working layer | count=%s", inserted)

    def _fetch_working_pairs(self) -> list[dict[str, Any]]:
        return fetch_all(
            sql=self.get_working_pairs_sql,
            api_file_name=self.mysql_api_file,
        )

    def _build_symbol_cache(self, working_pairs: list[dict[str, Any]]) -> dict[str, pd.DataFrame]:
        unique_symbols = sorted(
            {row["asset_1"] for row in working_pairs}.union({row["asset_2"] for row in working_pairs})
        )

        symbol_cache: dict[str, pd.DataFrame] = {}

        for symbol in unique_symbols:
            if not parquet_exists(symbol):
                self.logger.warning("Missing parquet for symbol; skipping in pair_state | symbol=%s", symbol)
                continue

            df = read_symbol_ohlcv_parquet(symbol).copy()
            df = self._normalize_ohlcv_frame(df)
            if len(df) < self.lookback_candles:
                self.logger.warning(
                    "Not enough parquet rows for pair_state | symbol=%s | rows=%s",
                    symbol,
                    len(df),
                )
                continue

            symbol_cache[symbol] = df.tail(self.lookback_candles).reset_index(drop=True)

        return symbol_cache

    def _fetch_trade_map_for_pairs(self, uuids: list[str]) -> dict[str, list[dict[str, Any]]]:
        if not uuids:
            return {}

        placeholders, params = build_not_in_params(uuids)
        sql = self.get_trade_res_for_pairs_sql_template.format(uuid_placeholders=placeholders)

        rows = fetch_all(
            sql=sql,
            api_file_name=self.mysql_api_file,
            params=params,
        )

        mapped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            mapped.setdefault(row["uuid"], []).append(row)
        return mapped

    def _build_pair_state_metrics(
        self,
        pair_row: dict[str, Any],
        symbol_cache: dict[str, pd.DataFrame],
        raw_trade_rows: list[dict[str, Any]],
        now_utc: datetime,
    ) -> dict[str, Any]:
        uuid = pair_row["uuid"]
        asset_1 = pair_row["asset_1"]
        asset_2 = pair_row["asset_2"]

        if asset_1 not in symbol_cache or asset_2 not in symbol_cache:
            raise ValueError(f"Missing symbol cache for uuid={uuid}")

        df_1 = symbol_cache[asset_1]
        df_2 = symbol_cache[asset_2]

        aligned = align_close_series(df_1, df_2, ts_column="ts", close_column="close")
        if len(aligned) < self.lookback_candles:
            raise ValueError(f"Not enough aligned candles for uuid={uuid}")

        beta = calculate_beta_ols(
            price_1=aligned["close_1"],
            price_2=aligned["close_2"],
            use_log=self.use_log_prices_for_beta,
        )
        beta_norm = normalize_beta(beta)

        spread = build_spread_from_beta(
            price_1=aligned["close_1"],
            price_2=aligned["close_2"],
            beta=beta,
            use_log=self.use_log_prices_for_beta,
        )

        adf_result = run_adf_test_from_series(
            series=spread,
            alpha=self.p_value_threshold,
            use_log=False,
        )
        hurst = self._safe_stat(lambda: calculate_hurst_exponent(spread))
        hl = self._safe_stat(lambda: calculate_half_life(spread))
        spread_skew = self._safe_stat(lambda: calculate_spread_skew(spread))
        spread_kurt = self._safe_stat(lambda: calculate_spread_kurt(spread))

        hl_spread_med = self._calculate_hl_spread_median(
            aligned=aligned,
            beta=beta,
            hl_value=hl,
        )
        last_spread = float(spread.iloc[-1])

        normalized_events = self._normalize_trade_events(raw_trade_rows)
        metrics_30 = self._build_window_trade_metrics(normalized_events, now_utc, days=30)
        metrics_180 = self._build_window_trade_metrics(normalized_events, now_utc, days=180)

        is_cointegrated = self._is_cointegrated(
            adf=adf_result["adf"],
            p_value=adf_result["p_value"],
        )

        level_30 = self._resolve_level_30(is_cointegrated, metrics_30)
        level_180 = self._resolve_level_180(is_cointegrated, metrics_30, metrics_180)

        quarantine_until = None
        quarantine_reason = None
        mark_quarantine = 0

        if self._has_losing_days_streak(normalized_events, now_utc, self.quarantine_losing_days_streak):
            quarantine_until = now_utc + timedelta(days=self.quarantine_days)
            quarantine_reason = f"losing_{self.quarantine_losing_days_streak}_days_row"
            level_30 = "quarantine"
            level_180 = "quarantine"
            mark_quarantine = 1

        remove_from_working_layer = 0
        if metrics_180["num_trades"] >= self.removal_min_trades and metrics_180["expect"] < 0:
            remove_from_working_layer = 1

        activity_score = float(metrics_30["num_trades"] + metrics_180["num_trades"])

        return {
            "uuid": uuid,
            "adf": adf_result["adf"],
            "p_value": adf_result["p_value"],
            "hurst": hurst,
            "hl": self._safe_int(hl),
            "spread_skew": spread_skew,
            "spread_kurt": spread_kurt,
            "beta": beta,
            "beta_norm": beta_norm,
            "hl_spread_med": hl_spread_med,
            "last_spread": last_spread,

            "win_rate_180": metrics_180["win_rate"],
            "rew_risk_180": metrics_180["rew_risk"],
            "num_trades_180": metrics_180["num_trades"],
            "total_pnl_180": metrics_180["total_pnl"],
            "expect_180": metrics_180["expect"],
            "level_180": level_180,

            "win_rate_30": metrics_30["win_rate"],
            "rew_risk_30": metrics_30["rew_risk"],
            "num_trades_30": metrics_30["num_trades"],
            "total_pnl_30": metrics_30["total_pnl"],
            "expect_30": metrics_30["expect"],
            "level_30": level_30,

            "quarantine_until": quarantine_until,
            "quarantine_reason": quarantine_reason,
            "activity_score": activity_score,
            "last_update_ts": now_utc,

            "_mark_quarantine": mark_quarantine,
            "_remove_from_working_layer": remove_from_working_layer,
        }

    def _normalize_trade_events(self, raw_trade_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not raw_trade_rows:
            return []

        rows = []
        for row in raw_trade_rows:
            open_dt = self._coerce_datetime(row.get("open_dt"))
            close_dt = self._coerce_datetime(row.get("close_dt"))
            if open_dt is None:
                continue

            rows.append(
                {
                    "uuid": row["uuid"],
                    "open_dt": open_dt,
                    "close_dt": close_dt,
                    "pnl": float(row.get("pnl") or 0.0),
                    "pnl_pers": float(row.get("pnl_pers") or 0.0),
                    "open_cond": str(row.get("open_cond") or ""),
                }
            )

        rows.sort(key=lambda x: (x["uuid"], x["open_cond"], x["open_dt"]))

        normalized: list[dict[str, Any]] = []
        current_group: list[dict[str, Any]] = []

        def flush_group(group: list[dict[str, Any]]) -> None:
            if not group:
                return
            normalized.append(
                {
                    "uuid": group[0]["uuid"],
                    "open_cond": group[0]["open_cond"],
                    "open_dt": min(x["open_dt"] for x in group),
                    "close_dt": self._max_nullable_dt([x["close_dt"] for x in group]),
                    "pnl": sum(x["pnl"] for x in group) / len(group),
                    "pnl_pers": sum(x["pnl_pers"] for x in group) / len(group),
                    "raw_trade_count": len(group),
                }
            )

        for row in rows:
            if not current_group:
                current_group = [row]
                continue

            prev = current_group[-1]
            same_uuid = row["uuid"] == prev["uuid"]
            same_open_cond = row["open_cond"] == prev["open_cond"]
            within_window = abs((row["open_dt"] - prev["open_dt"]).total_seconds()) <= self.same_event_window_minutes * 60

            if same_uuid and same_open_cond and within_window:
                current_group.append(row)
            else:
                flush_group(current_group)
                current_group = [row]

        flush_group(current_group)
        return normalized

    def _build_window_trade_metrics(
        self,
        normalized_events: list[dict[str, Any]],
        now_utc: datetime,
        days: int,
    ) -> dict[str, float | int]:
        cutoff = now_utc - timedelta(days=days)

        events = [x for x in normalized_events if x["open_dt"] >= cutoff]
        if not events:
            return {
                "win_rate": 0.0,
                "rew_risk": 0.0,
                "num_trades": 0,
                "total_pnl": 0.0,
                "expect": 0.0,
            }

        pnl_values = [float(x["pnl_pers"]) for x in events]
        wins = [x for x in pnl_values if x > 0]
        losses = [x for x in pnl_values if x < 0]

        num_trades = len(events)
        win_rate = len(wins) / num_trades if num_trades > 0 else 0.0
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss_abs = abs(sum(losses) / len(losses)) if losses else 0.0

        if avg_loss_abs > 0:
            rew_risk = avg_win / avg_loss_abs
        else:
            rew_risk = avg_win if avg_win > 0 else 0.0

        expect = (win_rate * rew_risk) - (1 - win_rate)
        total_pnl = sum(float(x["pnl"]) for x in events)

        return {
            "win_rate": float(win_rate),
            "rew_risk": float(rew_risk),
            "num_trades": int(num_trades),
            "total_pnl": float(total_pnl),
            "expect": float(expect),
        }

    def _has_losing_days_streak(
        self,
        normalized_events: list[dict[str, Any]],
        now_utc: datetime,
        streak_required: int,
    ) -> bool:
        if not normalized_events:
            return False

        by_day: dict[datetime.date, float] = {}
        for event in normalized_events:
            close_dt = event["close_dt"] or event["open_dt"]
            close_day = close_dt.date()
            by_day[close_day] = by_day.get(close_day, 0.0) + float(event["pnl"])

        if not by_day:
            return False

        losing_streak = 0
        for day in sorted(by_day.keys(), reverse=True):
            if by_day[day] < 0:
                losing_streak += 1
                if losing_streak >= streak_required:
                    return True
            else:
                break

        return False

    def _resolve_level_30(self, is_cointegrated: bool, metrics_30: dict[str, Any]) -> str:
        if not is_cointegrated:
            return "level_0"

        if metrics_30["num_trades"] > self.level2_30_min_trades and metrics_30["expect"] > 0:
            return "level_2"

        return "level_1"

    def _resolve_level_180(
        self,
        is_cointegrated: bool,
        metrics_30: dict[str, Any],
        metrics_180: dict[str, Any],
    ) -> str:
        if not is_cointegrated:
            return "level_0"

        if (
            metrics_180["num_trades"] > self.level2_180_min_trades
            and metrics_180["expect"] > 0
            and metrics_30["expect"] > 0
        ):
            return "level_2"

        return "level_1"

    def _mark_pair_universe_quarantine(self, uuids: list[str], now_utc: datetime) -> None:
        params_seq = [{"uuid": uuid, "dt_status_change": now_utc} for uuid in uuids]
        execute_many(
            sql=self.mark_pair_universe_quarantine_sql,
            api_file_name=self.mysql_api_file,
            params_seq=params_seq,
        )

    def _remove_bad_pairs_from_working_layer(self, uuids: list[str], now_utc: datetime) -> None:
        for uuid in uuids:
            execute(
                sql=self.delete_signal_table_pair_sql,
                api_file_name=self.mysql_api_file,
                params={"uuid": uuid},
            )
            execute(
                sql=self.delete_pair_state_pair_sql,
                api_file_name=self.mysql_api_file,
                params={"uuid": uuid},
            )

        params_seq = [{"uuid": uuid, "dt_status_change": now_utc} for uuid in uuids]
        execute_many(
            sql=self.mark_pair_universe_removed_sql,
            api_file_name=self.mysql_api_file,
            params_seq=params_seq,
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

    @staticmethod
    def _normalize_ohlcv_frame(df: pd.DataFrame) -> pd.DataFrame:
        required_cols = {"ts", "open", "high", "low", "close", "volume"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"OHLCV parquet is missing columns: {sorted(missing)}")

        normalized = df.copy()
        normalized["ts"] = pd.to_numeric(normalized["ts"], errors="coerce")
        normalized["open"] = pd.to_numeric(normalized["open"], errors="coerce")
        normalized["high"] = pd.to_numeric(normalized["high"], errors="coerce")
        normalized["low"] = pd.to_numeric(normalized["low"], errors="coerce")
        normalized["close"] = pd.to_numeric(normalized["close"], errors="coerce")
        normalized["volume"] = pd.to_numeric(normalized["volume"], errors="coerce")
        normalized = normalized.dropna(subset=["ts", "close"])
        normalized = normalized.drop_duplicates(subset=["ts"], keep="last")
        normalized = normalized.sort_values("ts").reset_index(drop=True)
        return normalized

    def _calculate_hl_spread_median(
        self,
        aligned: pd.DataFrame,
        beta: float,
        hl_value: float | None,
    ) -> float | None:
        if hl_value is None or math.isnan(hl_value) or hl_value <= 0:
            return None

        ratio = aligned["close_1"] / (float(beta) * aligned["close_2"])
        ratio = pd.to_numeric(ratio, errors="coerce").dropna()

        if ratio.empty:
            return None

        window = int(min(max(1, int(round(hl_value))), len(ratio)))
        return float(abs(ratio.tail(window).median()))

    def _is_cointegrated(self, adf: float | None, p_value: float | None) -> bool:
        if adf is None or p_value is None:
            return False
        return float(adf) < self.adf_threshold and float(p_value) < self.p_value_threshold

    @staticmethod
    def _safe_stat(fn):
        try:
            value = fn()
            return None if value is None else float(value)
        except Exception:
            return None

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(round(float(value)))
        except Exception:
            return None

    @staticmethod
    def _max_nullable_dt(values: list[datetime | None]) -> datetime | None:
        filtered = [x for x in values if x is not None]
        return max(filtered) if filtered else None

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
    def _parse_bool(value: str) -> bool:
        normalized = str(value).strip().lower()
        return normalized in {"1", "true", "yes", "y"}

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
    worker = PairStateWorker()
    worker.run_forever()