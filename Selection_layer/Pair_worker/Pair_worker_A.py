from __future__ import annotations

import itertools
import time
from pathlib import Path
from typing import Any

import pandas as pd

from Common.config.path_config import get_project_root
from Common.config.rules_loader import load_rules_file
from Common.db.db_execute import execute, execute_many, fetch_all
from Common.db.deadlock_retry import run_with_deadlock_retry
from Common.db.heartbeat_writer import write_heartbeat
from Common.exchange.bybit_client import create_bybit_client, fetch_ohlcv_with_retry
from Common.statistics.adf_test import run_adf_test_from_series
from Common.statistics.beta_calc import (
    build_spread_from_dfs,
    calculate_beta_from_dfs,
    normalize_beta,
)
from Common.statistics.half_life import calculate_half_life
from Common.statistics.hurst import calculate_hurst_exponent
from Common.statistics.scoring import score_stat_test
from Common.statistics.spread_stats import calculate_spread_stats
from Common.utils.cleanup import cleanup_objects, force_gc
from Common.utils.logger import setup_logger
from Common.utils.sql_file_loader import load_sql_file


VALID_SLEEP_STATUSES = {"SLEEP", "STOP", "SL_BLOCK"}


def str_to_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_optional_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.upper() == "NONE":
        return None
    return float(text)


def build_pair_uuid(asset_1: str, asset_2: str) -> str:
    def base_symbol(symbol: str) -> str:
        return symbol.split("/")[0].strip().upper()

    ordered = sorted([base_symbol(asset_1), base_symbol(asset_2)])
    return f"{ordered[0]}_{ordered[1]}"


def normalize_ohlcv_frame(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = {"ts", "open", "high", "low", "close", "volume"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"OHLCV parquet is missing columns: {sorted(missing)}")

    normalized = df.copy()
    for col in ["ts", "open", "high", "low", "close", "volume"]:
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce")

    normalized = normalized.dropna(subset=["ts", "close", "volume"])
    normalized = normalized.drop_duplicates(subset=["ts"], keep="last")
    normalized = normalized.sort_values("ts").reset_index(drop=True)
    return normalized


class PairWorkerA:
    def __init__(self) -> None:
        self.project_root = get_project_root()
        self.worker_dir = Path(__file__).resolve().parent
        self.rules_dir = self.worker_dir / "rules"
        self.sql_dir = self.worker_dir / "sql_queries"

        self.rules = self._load_rules()

        self.worker_name = self.rules.get("WORKER_A_NAME", "pair_worker_a")
        self.mysql_api_file = self.rules["MYSQL_API_FILE"]
        self.bybit_api_file = self.rules["BYBIT_API_FILE"]
        self.timeframe = self.rules.get("TIMEFRAME_A", "1h")
        self.target_candles = int(self.rules.get("TARGET_CANDLES_A", "1000"))
        self.scheduler_sleep_check_sec = int(self.rules.get("SCHEDULER_SLEEP_CHECK_SEC", "300"))
        self.pair_batch_insert_size = int(self.rules.get("PAIR_BATCH_INSERT_SIZE", "500"))
        self.stat_batch_size = int(self.rules.get("STAT_BATCH_SIZE", "50"))
        self.allow_stat_retry = str_to_bool(self.rules.get("ALLOW_STAT_RETRY", "true"))
        self.stat_retry_cooldown_hours = int(self.rules.get("STAT_RETRY_COOLDOWN_HOURS", "24"))
        self.selection_parquet_dir = self.project_root / self.rules.get(
            "SELECTION_PARQUET_DIR",
            "data/parquet_db_select",
        )

        log_file = self.project_root / self.rules.get("LOG_FILE_A", "data/logs/pair_worker_a.log")
        self.logger = setup_logger("pair_worker_a", log_file)

        self.bybit_client = create_bybit_client(self.bybit_api_file)

        self.sql_get_scheduler_statuses = load_sql_file(self.sql_dir / "get_scheduler_statuses.txt")
        self.sql_select_control_counts = load_sql_file(self.sql_dir / "select_control_counts.txt")
        self.sql_reset_old_forbidden_fail_pairs = load_sql_file(
            self.sql_dir / "reset_old_forbidden_fail_pairs.txt"
        )
        self.sql_mark_invalid_pairs_removed = load_sql_file(
            self.sql_dir / "mark_invalid_pairs_removed.txt"
        )
        self.sql_select_reliable_assets = load_sql_file(
            self.sql_dir / "select_reliable_assets_for_pair_creation.txt"
        )
        self.sql_count_existing_pairs = load_sql_file(
            self.sql_dir / "count_existing_pairs_for_reliable_assets.txt"
        )
        self.sql_insert_new_pairs_ignore = load_sql_file(
            self.sql_dir / "insert_new_pairs_ignore.txt"
        )
        self.sql_select_pairs_for_stat_test_global = load_sql_file(
            self.sql_dir / "select_pairs_for_stat_test_global.txt"
        )
        self.sql_update_pair_stat_state_running = load_sql_file(
            self.sql_dir / "update_pair_stat_state_running.txt"
        )
        self.sql_update_pair_stat_test_result = load_sql_file(
            self.sql_dir / "update_pair_stat_test_result.txt"
        )
        self.sql_update_pair_stat_test_fail = load_sql_file(
            self.sql_dir / "update_pair_stat_test_fail.txt"
        )

    def _load_rules(self) -> dict[str, str]:
        return load_rules_file(self.rules_dir / "pair_rules.txt")

    def reload_rules_for_loop(self) -> None:
        self.rules = self._load_rules()
        self.worker_name = self.rules.get("WORKER_A_NAME", "pair_worker_a")
        self.mysql_api_file = self.rules["MYSQL_API_FILE"]
        self.bybit_api_file = self.rules["BYBIT_API_FILE"]
        self.timeframe = self.rules.get("TIMEFRAME_A", "1h")
        self.target_candles = int(self.rules.get("TARGET_CANDLES_A", "1000"))
        self.scheduler_sleep_check_sec = int(self.rules.get("SCHEDULER_SLEEP_CHECK_SEC", "300"))
        self.pair_batch_insert_size = int(self.rules.get("PAIR_BATCH_INSERT_SIZE", "500"))
        self.stat_batch_size = int(self.rules.get("STAT_BATCH_SIZE", "50"))
        self.allow_stat_retry = str_to_bool(self.rules.get("ALLOW_STAT_RETRY", "true"))
        self.stat_retry_cooldown_hours = int(self.rules.get("STAT_RETRY_COOLDOWN_HOURS", "24"))
        self.selection_parquet_dir = self.project_root / self.rules.get(
            "SELECTION_PARQUET_DIR",
            "data/parquet_db_select",
        )

    def _execute(self, sql: str, params: Any | None = None) -> Any:
        return run_with_deadlock_retry(
            lambda: execute(
                sql=sql,
                api_file_name=self.mysql_api_file,
                params=params,
            )
        )

    def _execute_many(self, sql: str, params_seq: list[Any]) -> Any:
        return run_with_deadlock_retry(
            lambda: execute_many(
                sql=sql,
                api_file_name=self.mysql_api_file,
                params_seq=params_seq,
            )
        )

    def _safe_write_heartbeat(self, runtime_status: str, comment: str | None) -> None:
        try:
            write_heartbeat(
                worker_id=self.worker_name,
                runtime_status=runtime_status,
                comment=self._truncate_comment(comment),
                api_file_name=self.mysql_api_file,
            )
        except Exception:
            self.logger.exception(
                "Failed to write heartbeat | worker=%s | runtime_status=%s | comment=%s",
                self.worker_name,
                runtime_status,
                comment,
            )

    def _get_effective_control_status(self) -> tuple[str, str]:
        rows = fetch_all(
            sql=self.sql_get_scheduler_statuses,
            api_file_name=self.mysql_api_file,
            params={"worker_id": self.worker_name},
        )

        mapped = {row["worker_id"]: row for row in rows}
        global_row = mapped.get("GLOBAL")
        worker_row = mapped.get(self.worker_name)

        if global_row and str(global_row.get("control_status", "")).upper() in VALID_SLEEP_STATUSES:
            return str(global_row["control_status"]).upper(), str(global_row.get("comment") or "")

        if worker_row:
            return str(worker_row.get("control_status", "RUNNING")).upper(), str(
                worker_row.get("comment") or ""
            )

        return "RUNNING", "default_missing_scheduler_row"

    @staticmethod
    def _truncate_comment(comment: str | None, max_len: int = 64) -> str | None:
        if comment is None:
            return None
        return str(comment)[:max_len]

    def reset_old_forbidden_fail_pairs(self) -> int:
        sql = self.sql_reset_old_forbidden_fail_pairs.format(
            pair_retention_days_forbidden_fail=int(
                self.rules.get("PAIR_RETENTION_DAYS_FORBIDDEN_FAIL", "30")
            )
        )
        rows_affected = self._execute(sql=sql)
        self.logger.info("Reusable pair reset pass done | rows_affected=%s", rows_affected)
        return int(rows_affected or 0)

    def mark_invalid_pairs_removed(self) -> int:
        rows_affected = self._execute(sql=self.sql_mark_invalid_pairs_removed)
        self.logger.info("Invalid pair cleanup done | rows_affected=%s", rows_affected)
        return int(rows_affected or 0)

    def fetch_control_counts(self) -> dict[str, int]:
        rows = fetch_all(
            sql=self.sql_select_control_counts,
            api_file_name=self.mysql_api_file,
        )
        row = rows[0] if rows else {}
        return {
            "working_count": int(row.get("working_count") or 0),
            "candidate_count": int(row.get("candidate_count") or 0),
            "bt_pending_count": int(row.get("bt_pending_count") or 0),
        }

    def select_reliable_assets(self) -> list[dict[str, Any]]:
        rows = fetch_all(
            sql=self.sql_select_reliable_assets,
            api_file_name=self.mysql_api_file,
        )

        min_liq_5m = float(self.rules.get("MIN_LIQ_5M", "0"))
        min_liq_1h = float(self.rules.get("MIN_LIQ_1H", "0"))

        filtered = [
            row for row in rows
            if float(row.get("liq_5min_mean") or 0.0) >= min_liq_5m
            and float(row.get("liq_1h_mean") or 0.0) >= min_liq_1h
        ]

        self.logger.info(
            "Reliable assets selected | raw=%s filtered=%s",
            len(rows),
            len(filtered),
        )
        return filtered

    def count_existing_pairs_for_assets(self, asset_symbols: list[str]) -> int:
        if len(asset_symbols) < 2:
            return 0

        placeholders = ",".join(["%s"] * len(asset_symbols))
        sql = self.sql_count_existing_pairs.format(asset_placeholders=placeholders)
        params = tuple(asset_symbols) + tuple(asset_symbols)

        rows = fetch_all(
            sql=sql,
            api_file_name=self.mysql_api_file,
            params=params,
        )
        return int(rows[0]["existing_pair_count"]) if rows else 0

    def create_missing_pairs_for_reliable_assets(self, assets: list[dict[str, Any]]) -> int:
        if len(assets) < 2:
            return 0

        asset_info = {row["symbol"]: row for row in assets}
        symbols = sorted(asset_info.keys())

        pair_rows: list[tuple[Any, ...]] = []

        for asset_a, asset_b in itertools.combinations(symbols, 2):
            asset_1, asset_2 = sorted([asset_a, asset_b])

            row_1 = asset_info[asset_1]
            row_2 = asset_info[asset_2]

            pair_uuid = build_pair_uuid(asset_1, asset_2)
            pair_liq_5m = float(min(row_1["liq_5min_mean"], row_2["liq_5min_mean"]))
            pair_liq_1h = float(min(row_1["liq_1h_mean"], row_2["liq_1h_mean"]))
            activity_score = pair_liq_1h

            pair_rows.append(
                (
                    pair_uuid,
                    asset_1,
                    asset_2,
                    pair_liq_5m,
                    pair_liq_1h,
                    activity_score,
                )
            )

        inserted_estimate = 0
        for start in range(0, len(pair_rows), self.pair_batch_insert_size):
            batch = pair_rows[start:start + self.pair_batch_insert_size]
            rows_inserted = self._execute_many(
                sql=self.sql_insert_new_pairs_ignore,
                params_seq=batch,
            )
            inserted_estimate += int(rows_inserted or 0)

        self.logger.info(
            "Reliable-pair creation pass done | theoretical_pairs=%s inserted_estimate=%s",
            len(pair_rows),
            inserted_estimate,
        )
        return inserted_estimate

    def _selection_parquet_file_path(self, symbol: str) -> Path:
        safe_name = symbol.replace("/", "_").replace(":", "_")
        return self.selection_parquet_dir / f"{safe_name}.parquet"

    def _refresh_selection_parquet(self, symbol: str) -> pd.DataFrame:
        self.logger.info("Refreshing selection parquet | symbol=%s", symbol)

        rows = fetch_ohlcv_with_retry(
            bybit_client=self.bybit_client,
            symbol=symbol,
            timeframe=self.timeframe,
            limit=self.target_candles,
        )

        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        df = normalize_ohlcv_frame(df)

        self.selection_parquet_dir.mkdir(parents=True, exist_ok=True)
        file_path = self._selection_parquet_file_path(symbol)
        df.to_parquet(file_path, index=False)

        return df

    def build_selection_symbol_cache(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        cache: dict[str, pd.DataFrame] = {}

        for symbol in sorted(set(symbols)):
            try:
                cache[symbol] = self._refresh_selection_parquet(symbol)
            except Exception:
                self.logger.exception("Failed building selection parquet cache | symbol=%s", symbol)

        return cache

    def select_pairs_for_stat_test(self) -> list[dict[str, Any]]:
        sql = self.sql_select_pairs_for_stat_test_global.format(
            allow_stat_retry=1 if self.allow_stat_retry else 0,
            stat_retry_cooldown_hours=self.stat_retry_cooldown_hours,
            stat_batch_size=self.stat_batch_size,
        )

        rows = fetch_all(
            sql=sql,
            api_file_name=self.mysql_api_file,
        )
        return rows

    def _passes_optional_min(self, value: float, rule_value: str | None) -> bool:
        threshold = parse_optional_float(rule_value)
        if threshold is None:
            return True
        return float(value) >= threshold

    def _passes_optional_max(self, value: float, rule_value: str | None) -> bool:
        threshold = parse_optional_float(rule_value)
        if threshold is None:
            return True
        return float(value) <= threshold

    def evaluate_stat_thresholds(
        self,
        adf: float,
        p_value: float,
        hurst: float,
        hl: float,
        spread_skew: float,
        spread_kurt: float,
        beta: float,
        beta_norm: float,
        stat_test_score: float,
    ) -> bool:
        checks = [
            self._passes_optional_max(adf, self.rules.get("STAT_MAX_ADF")),
            self._passes_optional_max(p_value, self.rules.get("STAT_MAX_PVALUE")),
            self._passes_optional_max(hurst, self.rules.get("STAT_MAX_HURST")),
            self._passes_optional_min(hl, self.rules.get("STAT_MIN_HL")),
            self._passes_optional_max(hl, self.rules.get("STAT_MAX_HL")),
            self._passes_optional_min(spread_skew, self.rules.get("STAT_MIN_SPREAD_SKEW")),
            self._passes_optional_max(spread_skew, self.rules.get("STAT_MAX_SPREAD_SKEW")),
            self._passes_optional_min(spread_kurt, self.rules.get("STAT_MIN_SPREAD_KURT")),
            self._passes_optional_max(spread_kurt, self.rules.get("STAT_MAX_SPREAD_KURT")),
            self._passes_optional_min(beta, self.rules.get("STAT_MIN_BETA")),
            self._passes_optional_max(beta, self.rules.get("STAT_MAX_BETA")),
            self._passes_optional_min(beta_norm, self.rules.get("STAT_MIN_BETA_NORM")),
            self._passes_optional_max(beta_norm, self.rules.get("STAT_MAX_BETA_NORM")),
        ]

        min_stat_score = parse_optional_float(self.rules.get("MIN_STAT_TEST_SCORE"))
        if min_stat_score is not None:
            checks.append(float(stat_test_score) >= min_stat_score)

        return all(checks)

    def run_stat_test_for_pair(
        self,
        pair_row: dict[str, Any],
        symbol_cache: dict[str, pd.DataFrame],
    ) -> bool:
        pair_id = pair_row["id"]
        asset_1 = pair_row["asset_1"]
        asset_2 = pair_row["asset_2"]

        self._execute(
            sql=self.sql_update_pair_stat_state_running,
            params=(pair_id,),
        )

        try:
            if asset_1 not in symbol_cache or asset_2 not in symbol_cache:
                raise ValueError(f"Missing selection parquet cache for pair_id={pair_id}")

            df_1 = symbol_cache[asset_1]
            df_2 = symbol_cache[asset_2]

            beta = calculate_beta_from_dfs(df_1=df_1, df_2=df_2, use_log=True)
            beta_norm = normalize_beta(beta)

            spread_df = build_spread_from_dfs(
                df_1=df_1,
                df_2=df_2,
                beta=beta,
                use_log=True,
            )
            spread_series = spread_df["spread"]

            adf_result = run_adf_test_from_series(
                series=spread_series,
                alpha=0.05,
                use_log=False,
            )
            spread_stats = calculate_spread_stats(spread_series)
            hurst_value = calculate_hurst_exponent(spread_series)
            hl_value = calculate_half_life(spread_series)

            stat_test_score = score_stat_test(
                p_value=float(adf_result["p_value"]),
                hurst=float(hurst_value),
                half_life=float(hl_value),
            )

            stat_pass = self.evaluate_stat_thresholds(
                adf=float(adf_result["adf"]),
                p_value=float(adf_result["p_value"]),
                hurst=float(hurst_value),
                hl=float(hl_value),
                spread_skew=float(spread_stats["spread_skew"]),
                spread_kurt=float(spread_stats["spread_kurt"]),
                beta=float(beta),
                beta_norm=float(beta_norm),
                stat_test_score=float(stat_test_score),
            )

            new_bt_state = "pending" if stat_pass else "fail"

            self._execute(
                sql=self.sql_update_pair_stat_test_result,
                params=(
                    float(adf_result["adf"]),
                    float(adf_result["p_value"]),
                    float(hurst_value),
                    float(hl_value),
                    float(spread_stats["spread_skew"]),
                    float(spread_stats["spread_kurt"]),
                    float(beta),
                    float(beta_norm),
                    float(stat_test_score),
                    "done",
                    new_bt_state,
                    pair_id,
                ),
            )

            self.logger.info(
                "Stat test done | pair_id=%s %s | %s adf=%.4f p=%.6f hurst=%.4f hl=%.2f score=%.4f bt_state=%s",
                pair_id,
                asset_1,
                asset_2,
                float(adf_result["adf"]),
                float(adf_result["p_value"]),
                float(hurst_value),
                float(hl_value),
                float(stat_test_score),
                new_bt_state,
            )

            cleanup_objects(spread_df, spread_series, adf_result, spread_stats)
            return stat_pass

        except Exception:
            self._execute(
                sql=self.sql_update_pair_stat_test_fail,
                params=(pair_id,),
            )
            self.logger.exception(
                "Stat test failed | pair_id=%s asset_1=%s asset_2=%s",
                pair_id,
                asset_1,
                asset_2,
            )
            return False

    def run_once(self) -> None:
        self.reload_rules_for_loop()

        reset_count = self.reset_old_forbidden_fail_pairs()
        removed_count = self.mark_invalid_pairs_removed()
        counts = self.fetch_control_counts()

        self.logger.info(
            "Control counts | working=%s candidate=%s bt_pending=%s reset=%s removed=%s",
            counts["working_count"],
            counts["candidate_count"],
            counts["bt_pending_count"],
            reset_count,
            removed_count,
        )

        reliable_assets = self.select_reliable_assets()
        reliable_symbols = [row["symbol"] for row in reliable_assets]
        reliable_count = len(reliable_symbols)

        if reliable_count < 2:
            self.logger.info("Insufficient reliable assets | count=%s", reliable_count)
            return

        theoretical_pair_count = reliable_count * (reliable_count - 1) // 2
        existing_pair_count = self.count_existing_pairs_for_assets(reliable_symbols)

        self.logger.info(
            "Reliable pair universe | assets=%s theoretical_pairs=%s existing_pairs=%s",
            reliable_count,
            theoretical_pair_count,
            existing_pair_count,
        )

        if existing_pair_count < theoretical_pair_count:
            self.create_missing_pairs_for_reliable_assets(reliable_assets)

        stat_pairs = self.select_pairs_for_stat_test()
        self.logger.info("Pairs selected for stat test | count=%s", len(stat_pairs))

        if not stat_pairs:
            return

        symbols_for_cache = sorted(
            {row["asset_1"] for row in stat_pairs}.union({row["asset_2"] for row in stat_pairs})
        )
        symbol_cache = self.build_selection_symbol_cache(symbols_for_cache)

        total_stat_processed = 0
        total_stat_passed = 0

        for row in stat_pairs:
            passed = self.run_stat_test_for_pair(row, symbol_cache)
            total_stat_processed += 1
            total_stat_passed += int(passed)
            force_gc()

        self.logger.info(
            "Pair worker A cycle complete | reliable_assets=%s stat_processed=%s stat_passed=%s",
            reliable_count,
            total_stat_processed,
            total_stat_passed,
        )

        cleanup_objects(reliable_assets, stat_pairs, symbol_cache)
        force_gc()

    def run_forever(self) -> None:
        self.logger.info(
            "Pair worker A started | timeframe=%s target_candles=%s stat_batch_size=%s parquet_dir=%s",
            self.timeframe,
            self.target_candles,
            self.stat_batch_size,
            self.selection_parquet_dir,
        )

        while True:
            try:
                control_status, control_comment = self._get_effective_control_status()

                if control_status in VALID_SLEEP_STATUSES:
                    self._safe_write_heartbeat(
                        runtime_status="SLEEPING",
                        comment=f"sleep_by_scheduler:{control_status}",
                    )
                    self.logger.info(
                        "Pair worker A sleeping by scheduler | status=%s | comment=%s",
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

                self.logger.info("Pair worker A loop finished | elapsed_sec=%.2f", elapsed)
                time.sleep(max(1.0, self.scheduler_sleep_check_sec))

            except Exception as exc:
                self.logger.exception("Pair worker A loop failed")
                self._safe_write_heartbeat(
                    runtime_status="ERROR",
                    comment=self._truncate_comment(f"loop_error:{type(exc).__name__}"),
                )
                time.sleep(10)

            finally:
                force_gc()


def main() -> None:
    worker = PairWorkerA()
    worker.run_forever()


if __name__ == "__main__":
    main()