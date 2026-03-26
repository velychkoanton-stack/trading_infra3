from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pandas as pd

from Common.backtest.pair_backtester import run_pair_backtest
from Common.config.csv_grid_loader import load_csv_grid
from Common.config.path_config import get_project_root
from Common.config.rules_loader import load_rules_file
from Common.db.db_execute import execute, fetch_all
from Common.db.deadlock_retry import run_with_deadlock_retry
from Common.db.heartbeat_writer import write_heartbeat
from Common.exchange.bybit_client import create_bybit_client, fetch_ohlcv_with_retry
from Common.parquet.parquet_reader import parquet_exists, read_symbol_ohlcv_parquet
from Common.parquet.parquet_updater import replace_symbol_ohlcv_parquet
from Common.utils.cleanup import cleanup_objects, force_gc
from Common.utils.logger import setup_logger
from Common.utils.sql_file_loader import load_sql_file


VALID_SLEEP_STATUSES = {"SLEEP", "STOP", "SL_BLOCK"}


def parse_optional_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.upper() == "NONE":
        return None
    return float(text)


class PairWorkerB:
    def __init__(self) -> None:
        self.project_root = get_project_root()
        self.worker_dir = Path(__file__).resolve().parent
        self.rules_dir = self.worker_dir / "rules"
        self.sql_dir = self.worker_dir / "sql_queries"

        self.rules = self._load_rules()

        self.worker_name = self.rules.get("WORKER_B_NAME", "pair_worker_b")
        self.mysql_api_file = self.rules.get("MYSQL_API_FILE", "api_mysql_main.txt")
        self.bybit_api_file = self.rules.get("BYBIT_API_FILE", "api_bybit_main.txt")
        self.timeframe = self.rules.get("TIMEFRAME_B", "5m")
        self.target_candles = int(self.rules.get("TARGET_CANDLES_B", "10000"))
        self.scheduler_sleep_check_sec = int(self.rules.get("SCHEDULER_SLEEP_CHECK_SEC", "300"))
        self.bt_batch_size = int(self.rules.get("BT_BATCH_SIZE", "10"))
        self.max_stat_age_days = int(self.rules.get("MAX_STAT_AGE_DAYS", "30"))
        self.parquet_max_age_sec_bt = int(
            self.rules.get("PARQUET_MAX_AGE_SEC_BT", str(7 * 24 * 60 * 60))
        )

        log_file = self.project_root / self.rules.get("LOG_FILE_B", "data/logs/pair_worker_b.log")
        self.logger = setup_logger("pair_worker_b", log_file)

        self.bybit_client = create_bybit_client(self.bybit_api_file)

        self.sql_get_scheduler_statuses = load_sql_file(self.sql_dir / "get_scheduler_statuses.txt")
        self.sql_select_pairs_for_backtest_queue = load_sql_file(
            self.sql_dir / "select_pairs_for_backtest_queue.txt"
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

        self.worker_name = self.rules.get("WORKER_B_NAME", "pair_worker_b")
        self.mysql_api_file = self.rules.get("MYSQL_API_FILE", "api_mysql_main.txt")
        self.bybit_api_file = self.rules.get("BYBIT_API_FILE", "api_bybit_main.txt")
        self.timeframe = self.rules.get("TIMEFRAME_B", "5m")
        self.target_candles = int(self.rules.get("TARGET_CANDLES_B", "10000"))
        self.scheduler_sleep_check_sec = int(self.rules.get("SCHEDULER_SLEEP_CHECK_SEC", "300"))
        self.bt_batch_size = int(self.rules.get("BT_BATCH_SIZE", "10"))
        self.max_stat_age_days = int(self.rules.get("MAX_STAT_AGE_DAYS", "30"))
        self.parquet_max_age_sec_bt = int(
            self.rules.get("PARQUET_MAX_AGE_SEC_BT", str(7 * 24 * 60 * 60))
        )

    def _execute(self, sql: str, params: Any | None = None) -> Any:
        return run_with_deadlock_retry(
            lambda: execute(
                sql=sql,
                api_file_name=self.mysql_api_file,
                params=params,
            )
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

    @staticmethod
    def _truncate_comment(comment: str | None, max_len: int = 64) -> str | None:
        if comment is None:
            return None
        return str(comment)[:max_len]

    def load_backtest_grid(self) -> pd.DataFrame:
        grid_file = self.rules["BACKTEST_GRID_FILE"]
        return load_csv_grid(self.rules_dir / grid_file)

    def select_pairs_for_backtest_queue(self) -> list[dict[str, Any]]:
        sql = self.sql_select_pairs_for_backtest_queue.format(
            max_stat_age_days=self.max_stat_age_days,
            bt_batch_size=self.bt_batch_size,
        )
        return fetch_all(
            sql=sql,
            api_file_name=self.mysql_api_file,
        )

    def _normalize_ohlcv_frame(self, df: pd.DataFrame) -> pd.DataFrame:
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
        normalized = normalized.dropna(subset=["ts", "close", "volume"])
        normalized = normalized.drop_duplicates(subset=["ts"], keep="last")
        normalized = normalized.sort_values("ts").reset_index(drop=True)
        return normalized

    def _load_or_refresh_symbol_parquet(self, symbol: str) -> pd.DataFrame:
        needs_refresh = True
        df: pd.DataFrame | None = None
        now_utc = pd.Timestamp.utcnow()

        if parquet_exists(symbol):
            try:
                df = read_symbol_ohlcv_parquet(symbol)
                df = self._normalize_ohlcv_frame(df)

                if len(df) >= self.target_candles:
                    last_ts = pd.to_numeric(df["ts"], errors="coerce").dropna()
                    if not last_ts.empty:
                        last_dt = pd.to_datetime(int(last_ts.iloc[-1]), unit="ms", utc=True)
                        age_sec = (now_utc - last_dt).total_seconds()
                        if age_sec <= self.parquet_max_age_sec_bt:
                            needs_refresh = False
            except Exception:
                self.logger.exception("Failed reading existing parquet | symbol=%s", symbol)
                df = None

        if needs_refresh:
            self.logger.info("Refreshing parquet for backtest worker | symbol=%s", symbol)
            rows = fetch_ohlcv_with_retry(
                bybit_client=self.bybit_client,
                symbol=symbol,
                timeframe=self.timeframe,
                limit=self.target_candles,
            )
            replace_symbol_ohlcv_parquet(symbol=symbol, rows=rows)
            df = read_symbol_ohlcv_parquet(symbol)
            df = self._normalize_ohlcv_frame(df)
            self.logger.info(f"Backtest candles count: {len(df)}")

        if df is None or df.empty:
            raise ValueError(f"Parquet unavailable after refresh attempt | symbol={symbol}")

        return df.tail(self.target_candles).reset_index(drop=True)

    def build_asset_data_for_pairs(self, pair_rows: list[dict[str, Any]]) -> dict[str, pd.DataFrame]:
        unique_symbols = sorted(
            {row["asset_1"] for row in pair_rows}.union({row["asset_2"] for row in pair_rows})
        )

        asset_data: dict[str, pd.DataFrame] = {}

        for symbol in unique_symbols:
            try:
                asset_data[symbol] = self._load_or_refresh_symbol_parquet(symbol)
            except Exception:
                self.logger.exception("Failed loading parquet for backtest | symbol=%s", symbol)

        return asset_data

    def run_backtest_for_pair(
        self,
        pair_row: dict[str, Any],
        asset_data: dict[str, pd.DataFrame],
        grid_df: pd.DataFrame,
    ) -> bool:
        pair_id = pair_row["id"]
        asset_1 = pair_row["asset_1"]
        asset_2 = pair_row["asset_2"]

        self._execute(
            sql=self.sql_update_pair_bt_state_running,
            params=(pair_id,),
        )

        try:
            if asset_1 not in asset_data or asset_2 not in asset_data:
                raise ValueError(f"Missing parquet data for pair_id={pair_id}")

            df_1 = asset_data[asset_1]
            df_2 = asset_data[asset_2]

            hl_value = float(pair_row["hl"])
            min_hl_window = int(self.rules.get("MIN_BACKTEST_HL_WINDOW", "50"))
            max_hl_window = int(self.rules.get("MAX_BACKTEST_HL_WINDOW", "500"))
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
                self._execute(
                    sql=self.sql_update_pair_backtest_fail,
                    params=("forbidden", pair_id),
                )
                self.logger.info(
                    "Backtest failed gate | pair_id=%s %s | %s reason=%s",
                    pair_id,
                    asset_1,
                    asset_2,
                    backtest_result["reason"],
                )
                return False

            selected = backtest_result["selected_result"]
            backtest_score = float(selected["backtest_score"])

            min_backtest_score = parse_optional_float(self.rules.get("MIN_BACKTEST_SCORE"))
            bt_pass = True if min_backtest_score is None else backtest_score >= min_backtest_score

            final_bt_state = "done" if bt_pass else "fail"
            final_status = "candidate" if bt_pass else "forbidden"

            self._execute(
                sql=self.sql_update_pair_backtest_result,
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
                    final_bt_state,
                    final_status,
                    pair_id,
                ),
            )

            self.logger.info(
                "Backtest done | pair_id=%s %s | %s status=%s bt_state=%s final_equity=%.4f win_rate=%.2f rr=%.4f trades=%s bt_score=%.4f pos_share=%.4f",
                pair_id,
                asset_1,
                asset_2,
                final_status,
                final_bt_state,
                float(selected["bt_final_equity"]),
                float(selected["win_rate"]),
                float(selected["risk_reward_ratio"]),
                int(selected["num_trades"]),
                float(backtest_score),
                float(backtest_result["positive_grid_share"]),
            )

            cleanup_objects(selected, backtest_result)
            return bt_pass

        except Exception:
            self._execute(
                sql=self.sql_update_pair_backtest_fail,
                params=("forbidden", pair_id),
            )
            self.logger.exception(
                "Backtest execution failed | pair_id=%s asset_1=%s asset_2=%s",
                pair_id,
                asset_1,
                asset_2,
            )
            return False

    def run_once(self) -> None:
        self.reload_rules_for_loop()
        grid_df = self.load_backtest_grid()

        bt_pairs = self.select_pairs_for_backtest_queue()
        self.logger.info("Pairs selected for backtest queue | count=%s", len(bt_pairs))

        if not bt_pairs:
            cleanup_objects(grid_df)
            return

        asset_data = self.build_asset_data_for_pairs(bt_pairs)

        if not asset_data:
            self.logger.info("No parquet-ready assets available for backtest batch")
            cleanup_objects(grid_df, bt_pairs, asset_data)
            force_gc()
            return

        total_processed = 0
        total_passed = 0

        for row in bt_pairs:
            passed = self.run_backtest_for_pair(row, asset_data, grid_df)
            total_processed += 1
            total_passed += int(passed)
            force_gc()

        self.logger.info(
            "Pair worker B cycle complete | selected=%s processed=%s passed=%s failed=%s",
            len(bt_pairs),
            total_processed,
            total_passed,
            total_processed - total_passed,
        )

        cleanup_objects(grid_df, bt_pairs, asset_data)
        force_gc()

    def run_forever(self) -> None:
        self.logger.info(
            "Pair worker B started | bt_batch_size=%s max_stat_age_days=%s parquet_max_age_sec_bt=%s",
            self.bt_batch_size,
            self.max_stat_age_days,
            self.parquet_max_age_sec_bt,
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
                        "Pair worker B sleeping by scheduler | status=%s | comment=%s",
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

                self.logger.info("Pair worker B loop finished | elapsed_sec=%.2f", elapsed)
                time.sleep(max(1.0, self.scheduler_sleep_check_sec))

            except Exception as exc:
                self.logger.exception("Pair worker B loop failed")
                self._safe_write_heartbeat(
                    runtime_status="ERROR",
                    comment=self._truncate_comment(f"loop_error:{type(exc).__name__}"),
                )
                time.sleep(10)

            finally:
                force_gc()


def main() -> None:
    worker = PairWorkerB()
    worker.run_forever()


if __name__ == "__main__":
    main()