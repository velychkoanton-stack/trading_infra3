from __future__ import annotations

import hashlib
import itertools
import time
from pathlib import Path
from typing import Any

import pandas as pd

from Common.backtest.pair_backtester import run_pair_backtest
from Common.config.csv_grid_loader import load_csv_grid
from Common.config.path_config import get_project_root
from Common.config.rules_loader import load_rules_file
from Common.db.db_execute import execute, execute_many, fetch_all
from Common.exchange.bybit_client import create_bybit_client, fetch_ohlcv_with_retry
from Common.parquet.parquet_reader import parquet_exists, read_symbol_ohlcv_parquet
from Common.parquet.parquet_updater import replace_symbol_ohlcv_parquet
from Common.statistics.adf_test import run_adf_test_from_series
from Common.statistics.beta_calc import build_spread_from_dfs, calculate_beta_from_dfs, normalize_beta
from Common.statistics.half_life import calculate_half_life
from Common.statistics.hurst import calculate_hurst_exponent
from Common.statistics.scoring import score_stat_test
from Common.statistics.spread_stats import calculate_spread_stats
from Common.utils.cleanup import cleanup_objects, force_gc
from Common.utils.logger import setup_logger
from Common.utils.sql_file_loader import load_sql_file


def str_to_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_optional_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.upper() == "NONE":
        return None
    return float(text)


def parse_optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.upper() == "NONE":
        return None
    return int(text)


def build_pair_uuid(asset_1: str, asset_2: str) -> str:
    """
    Deterministic pair UUID from ordered asset names.

    We store a stable SHA1 hash because raw symbols may exceed practical UUID length.
    """
    pair_key = f"{asset_1}|{asset_2}"
    return hashlib.sha1(pair_key.encode("utf-8")).hexdigest()


def safe_sleep(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)


class PairWorker:
    def __init__(self) -> None:
        self.project_root = get_project_root()
        self.worker_dir = Path(__file__).resolve().parent
        self.rules_dir = self.worker_dir / "rules"
        self.sql_dir = self.worker_dir / "sql_queries"

        self.rules = self._load_rules()

        log_file = self.project_root / self.rules["LOG_FILE"]
        self.logger = setup_logger("pair_worker", log_file)

        self.bybit_client = create_bybit_client(self.rules["BYBIT_API_FILE"])

        self.sql_select_asset_pool_primary = load_sql_file(
            self.sql_dir / "select_asset_pool_primary.txt"
        )
        self.sql_select_asset_pool_fallback = load_sql_file(
            self.sql_dir / "select_asset_pool_fallback.txt"
        )
        self.sql_insert_new_pairs_ignore = load_sql_file(
            self.sql_dir / "insert_new_pairs_ignore.txt"
        )
        self.sql_select_pairs_for_stat_test = load_sql_file(
            self.sql_dir / "select_pairs_for_stat_test.txt"
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
        self.sql_select_pairs_for_backtest = load_sql_file(
            self.sql_dir / "select_pairs_for_backtest.txt"
        )
        self.sql_update_pair_bt_state_running = load_sql_file(
            self.sql_dir / "update_pair_bt_state_running.txt"
        )
        self.sql_update_pair_backtest_result = load_sql_file(
            self.sql_dir / "update_pair_backtest_result.txt"
        )
        self.sql_update_pair_backtest_fail = load_sql_file(
            self.sql_dir / "update_pair_backtest_fail.txt"
        )

    def _load_rules(self) -> dict[str, str]:
        return load_rules_file(self.rules_dir / "pair_rules.txt")

    def reload_rules_for_loop(self) -> None:
        self.rules = self._load_rules()

    @property
    def mysql_api_file(self) -> str:
        return self.rules["MYSQL_API_FILE"]

    def load_backtest_grid(self) -> pd.DataFrame:
        grid_file = self.rules["BACKTEST_GRID_FILE"]
        return load_csv_grid(self.rules_dir / grid_file)

    def build_asset_filter_sql_parts(self) -> tuple[str, str]:
        asset_max_adf = parse_optional_float(self.rules.get("ASSET_MAX_ADF"))
        asset_max_pvalue = parse_optional_float(self.rules.get("ASSET_MAX_PVALUE"))

        asset_adf_filter_sql = ""
        asset_pvalue_filter_sql = ""

        if asset_max_adf is not None:
            asset_adf_filter_sql = f"AND a.adf <= {asset_max_adf}"

        if asset_max_pvalue is not None:
            asset_pvalue_filter_sql = f"AND a.p_value <= {asset_max_pvalue}"

        return asset_adf_filter_sql, asset_pvalue_filter_sql

    def select_asset_pool(self) -> list[dict[str, Any]]:
        asset_adf_filter_sql, asset_pvalue_filter_sql = self.build_asset_filter_sql_parts()

        sql_primary = self.sql_select_asset_pool_primary.format(
            asset_require_non_stationary_status=int(
                self.rules["ASSET_REQUIRE_NON_STATIONARY_STATUS"]
            ),
            min_liq_5m=float(self.rules["MIN_LIQ_5M"]),
            min_liq_1h=float(self.rules["MIN_LIQ_1H"]),
            max_asset_presence=float(self.rules["MAX_ASSET_PRESENCE"]),
            asset_pool_size=int(self.rules["ASSET_POOL_SIZE"]),
            asset_adf_filter_sql=asset_adf_filter_sql,
            asset_pvalue_filter_sql=asset_pvalue_filter_sql,
        )

        rows = fetch_all(sql=sql_primary, api_file_name=self.mysql_api_file)

        if len(rows) >= int(self.rules["ASSET_POOL_SIZE"]):
            return rows

        sql_fallback = self.sql_select_asset_pool_fallback.format(
            asset_require_non_stationary_status=int(
                self.rules["ASSET_REQUIRE_NON_STATIONARY_STATUS"]
            ),
            min_liq_5m=float(self.rules["MIN_LIQ_5M"]),
            min_liq_1h=float(self.rules["MIN_LIQ_1H"]),
            asset_pool_size=int(self.rules["ASSET_POOL_SIZE"]),
            asset_adf_filter_sql=asset_adf_filter_sql,
            asset_pvalue_filter_sql=asset_pvalue_filter_sql,
        )

        fallback_rows = fetch_all(sql=sql_fallback, api_file_name=self.mysql_api_file)
        return fallback_rows

    def ensure_pool_parquet_data(self, asset_pool: list[dict[str, Any]]) -> dict[str, pd.DataFrame]:
        asset_data: dict[str, pd.DataFrame] = {}
        target_candles = int(self.rules["TARGET_CANDLES"])
        timeframe = self.rules["TIMEFRAME"]
        allow_refresh = str_to_bool(self.rules["ALLOW_PARQUET_REFRESH"])
        parquet_max_age_sec = int(self.rules["PARQUET_MAX_AGE_SEC"])

        now_utc = pd.Timestamp.utcnow()

        for asset in asset_pool:
            symbol = asset["symbol"]
            df: pd.DataFrame | None = None
            needs_refresh = False

            if parquet_exists(symbol):
                df = read_symbol_ohlcv_parquet(symbol)

                if df.empty or "ts" not in df.columns or len(df) < target_candles:
                    needs_refresh = True
                else:
                    last_ts = pd.to_numeric(df["ts"], errors="coerce").dropna()
                    if last_ts.empty:
                        needs_refresh = True
                    else:
                        last_dt = pd.to_datetime(int(last_ts.iloc[-1]), unit="ms", utc=True)
                        age_sec = (now_utc - last_dt).total_seconds()
                        if age_sec > parquet_max_age_sec:
                            needs_refresh = True
            else:
                needs_refresh = True

            if needs_refresh and allow_refresh:
                self.logger.info("Refreshing parquet | symbol=%s", symbol)
                rows = fetch_ohlcv_with_retry(
                    bybit_client=self.bybit_client,
                    symbol=symbol,
                    timeframe=timeframe,
                    limit=target_candles,
                )
                replace_symbol_ohlcv_parquet(symbol=symbol, rows=rows)
                df = read_symbol_ohlcv_parquet(symbol)

            if df is None or df.empty:
                self.logger.warning("Parquet unavailable after refresh attempt | symbol=%s", symbol)
                continue

            asset_data[symbol] = df

        return asset_data

    def create_missing_pairs(self, asset_pool: list[dict[str, Any]]) -> int:
        if len(asset_pool) < 2:
            return 0

        pair_rows: list[tuple[Any, ...]] = []

        asset_info = {row["symbol"]: row for row in asset_pool}

        for asset_a, asset_b in itertools.combinations(sorted(asset_info.keys()), 2):
            asset_1, asset_2 = sorted([asset_a, asset_b])

            row_1 = asset_info[asset_1]
            row_2 = asset_info[asset_2]

            pair_uuid = build_pair_uuid(asset_1, asset_2)

            # Conservative pair liquidity/activity:
            # the weaker leg usually defines what is realistically tradable.
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

        if not pair_rows:
            return 0

        batch_size = int(self.rules["PAIR_BATCH_INSERT_SIZE"])
        inserted_estimate = 0

        for start in range(0, len(pair_rows), batch_size):
            batch = pair_rows[start : start + batch_size]
            execute_many(
                sql=self.sql_insert_new_pairs_ignore,
                api_file_name=self.mysql_api_file,
                params_seq=batch,
            )
            inserted_estimate += len(batch)

        return inserted_estimate

    def select_pairs_for_stat_test(self, asset_symbols: list[str]) -> list[dict[str, Any]]:
        placeholders = ",".join(["%s"] * len(asset_symbols))

        sql = self.sql_select_pairs_for_stat_test.format(
            asset_pool_placeholders=placeholders,
            allow_stat_retry=1 if str_to_bool(self.rules["ALLOW_STAT_RETRY"]) else 0,
            stat_retry_cooldown_hours=int(self.rules["STAT_RETRY_COOLDOWN_HOURS"]),
            stat_batch_size=int(self.rules["STAT_BATCH_SIZE"]),
        )

        params = tuple(asset_symbols) + tuple(asset_symbols)

        return fetch_all(
            sql=sql,
            api_file_name=self.mysql_api_file,
            params=params,
        )

    def select_pairs_for_backtest(self, asset_symbols: list[str]) -> list[dict[str, Any]]:
        placeholders = ",".join(["%s"] * len(asset_symbols))

        sql = self.sql_select_pairs_for_backtest.format(
            asset_pool_placeholders=placeholders,
            allow_bt_retry=1 if str_to_bool(self.rules["ALLOW_BT_RETRY"]) else 0,
            bt_retry_cooldown_hours=int(self.rules["BT_RETRY_COOLDOWN_HOURS"]),
            bt_batch_size=int(self.rules["BT_BATCH_SIZE"]),
        )

        params = tuple(asset_symbols) + tuple(asset_symbols)

        return fetch_all(
            sql=sql,
            api_file_name=self.mysql_api_file,
            params=params,
        )

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
        asset_data: dict[str, pd.DataFrame],
    ) -> None:
        pair_id = pair_row["id"]
        asset_1 = pair_row["asset_1"]
        asset_2 = pair_row["asset_2"]

        execute(
            sql=self.sql_update_pair_stat_state_running,
            api_file_name=self.mysql_api_file,
            params=(pair_id,),
        )

        try:
            df_1 = asset_data[asset_1]
            df_2 = asset_data[asset_2]

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

            execute(
                sql=self.sql_update_pair_stat_test_result,
                api_file_name=self.mysql_api_file,
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
                "Stat test done | pair_id=%s %s | %s beta=%.4f beta_norm=%.4f adf=%.4f p=%.6f hurst=%.4f hl=%.2f score=%.4f bt_state=%s",
                pair_id,
                asset_1,
                asset_2,
                float(beta),
                float(beta_norm),
                float(adf_result["adf"]),
                float(adf_result["p_value"]),
                float(hurst_value),
                float(hl_value),
                float(stat_test_score),
                new_bt_state,
            )

            cleanup_objects(spread_df, spread_series, adf_result, spread_stats)

        except Exception:
            execute(
                sql=self.sql_update_pair_stat_test_fail,
                api_file_name=self.mysql_api_file,
                params=(pair_id,),
            )
            self.logger.exception(
                "Stat test failed | pair_id=%s asset_1=%s asset_2=%s",
                pair_id,
                asset_1,
                asset_2,
            )

    def run_backtest_for_pair(
        self,
        pair_row: dict[str, Any],
        asset_data: dict[str, pd.DataFrame],
        grid_df: pd.DataFrame,
    ) -> None:
        pair_id = pair_row["id"]
        asset_1 = pair_row["asset_1"]
        asset_2 = pair_row["asset_2"]

        execute(
            sql=self.sql_update_pair_bt_state_running,
            api_file_name=self.mysql_api_file,
            params=(pair_id,),
        )

        try:
            df_1 = asset_data[asset_1]
            df_2 = asset_data[asset_2]

            hl_value = float(pair_row["hl"])
            min_hl_window = int(self.rules["MIN_BACKTEST_HL_WINDOW"])
            max_hl_window = int(self.rules["MAX_BACKTEST_HL_WINDOW"])

            rolling_window = max(min_hl_window, min(max_hl_window, int(round(hl_value))))

            backtest_result = run_pair_backtest(
                df_asset_1=df_1,
                df_asset_2=df_2,
                asset_1=asset_1,
                asset_2=asset_2,
                rolling_window=rolling_window,
                measured_beta_norm=float(pair_row["beta_norm"]),
                grid_df=grid_df,
                rules=self.rules,
            )

            if not backtest_result["success"]:
                execute(
                    sql=self.sql_update_pair_backtest_fail,
                    api_file_name=self.mysql_api_file,
                    params=("forbidden", pair_id),
                )
                self.logger.info(
                    "Backtest failed gate | pair_id=%s %s | %s reason=%s",
                    pair_id,
                    asset_1,
                    asset_2,
                    backtest_result["reason"],
                )
                return

            selected = backtest_result["selected_result"]
            backtest_score = float(selected["backtest_score"])

            min_backtest_score = parse_optional_float(self.rules.get("MIN_BACKTEST_SCORE"))
            bt_pass = True if min_backtest_score is None else backtest_score >= min_backtest_score

            final_status = "candidate" if bt_pass else "forbidden"

            execute(
                sql=self.sql_update_pair_backtest_result,
                api_file_name=self.mysql_api_file,
                params=(
                    float(selected["bt_final_equity"]),
                    float(selected["win_rate"]),
                    float(selected["risk_reward_ratio"]),
                    int(selected["num_trades"]),
                    int(selected["exit_mean_reverted"]),
                    int(selected["exit_hl_coint_lst"]),
                    float(selected["take_profit_percent"]),
                    float(selected["stop_loss_percent"]),
                    float(backtest_result["best_beta"]),
                    float(backtest_score),
                    "done",
                    final_status,
                    pair_id,
                ),
            )

            self.logger.info(
                "Backtest done | pair_id=%s %s | %s status=%s final_equity=%.4f win_rate=%.2f rr=%.4f trades=%s bt_score=%.4f pos_share=%.4f",
                pair_id,
                asset_1,
                asset_2,
                final_status,
                float(selected["bt_final_equity"]),
                float(selected["win_rate"]),
                float(selected["risk_reward_ratio"]),
                int(selected["num_trades"]),
                float(backtest_score),
                float(backtest_result["positive_grid_share"]),
            )

            cleanup_objects(selected, backtest_result)

        except Exception:
            execute(
                sql=self.sql_update_pair_backtest_fail,
                api_file_name=self.mysql_api_file,
                params=("forbidden", pair_id),
            )
            self.logger.exception(
                "Backtest execution failed | pair_id=%s asset_1=%s asset_2=%s",
                pair_id,
                asset_1,
                asset_2,
            )

    def run_loop(self) -> None:
        self.reload_rules_for_loop()
        grid_df = self.load_backtest_grid()

        asset_pool = self.select_asset_pool()
        if len(asset_pool) < 2:
            self.logger.info("Insufficient asset pool | count=%s", len(asset_pool))
            safe_sleep(float(self.rules["PAIR_LOOP_SLEEP_SEC"]))
            return

        asset_symbols = [row["symbol"] for row in asset_pool]
        self.logger.info("Selected asset pool | count=%s symbols=%s", len(asset_symbols), asset_symbols)

        asset_data = self.ensure_pool_parquet_data(asset_pool)
        available_symbols = sorted(asset_data.keys())

        if len(available_symbols) < 2:
            self.logger.info("Insufficient parquet-ready assets | count=%s", len(available_symbols)))
            cleanup_objects(asset_data, asset_pool, grid_df)
            force_gc()
            safe_sleep(float(self.rules["PAIR_LOOP_SLEEP_SEC"]))
            return

        parquet_asset_pool = [row for row in asset_pool if row["symbol"] in asset_data]
        created_pairs_estimate = self.create_missing_pairs(parquet_asset_pool)
        self.logger.info("Pair creation pass done | created_estimate=%s", created_pairs_estimate)

        stat_pairs = self.select_pairs_for_stat_test(available_symbols)
        self.logger.info("Pairs selected for stat test | count=%s", len(stat_pairs))

        for row in stat_pairs:
            self.run_stat_test_for_pair(row, asset_data)
            force_gc()

        bt_pairs = self.select_pairs_for_backtest(available_symbols)
        self.logger.info("Pairs selected for backtest | count=%s", len(bt_pairs))

        for row in bt_pairs:
            self.run_backtest_for_pair(row, asset_data, grid_df)
            force_gc()

        cleanup_objects(asset_data, parquet_asset_pool, asset_pool, grid_df, stat_pairs, bt_pairs)
        force_gc()

        safe_sleep(float(self.rules["PAIR_LOOP_SLEEP_SEC"]))

    def run_forever(self) -> None:
        self.logger.info("Pair worker started")

        while True:
            try:
                self.run_loop()
            except Exception:
                self.logger.exception("Pair worker loop failed")
                force_gc()
                safe_sleep(max(float(self.rules.get("PAIR_LOOP_SLEEP_SEC", 0)), 1.0))


def main() -> None:
    worker = PairWorker()
    worker.run_forever()


if __name__ == "__main__":
    main()