from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from Common.config.path_config import get_project_root
from Common.config.rules_loader import load_rules_file
from Common.db.db_execute import execute, execute_many, fetch_all
from Common.exchange.bybit_client import (
    build_not_in_params,
    create_bybit_client,
    fetch_linear_perpetual_symbols,
    fetch_ohlcv_with_retry,
)
from Common.parquet.parquet_writer import write_symbol_ohlcv_parquet
from Common.statistics.adf_test import run_adf_test_from_close_df
from Common.utils.cleanup import cleanup_objects, force_gc
from Common.utils.logger import setup_logger


def load_text_file(file_path: Path) -> str:
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    return file_path.read_text(encoding="utf-8").strip()


def str_to_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def build_ohlcv_dataframe(rows: list[list[Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    return df


def compute_liquidity_metrics(df: pd.DataFrame) -> tuple[float, float]:
    """
    liq_5min_mean:
        mean(close * volume) on last 1000 bars

    liq_1h_mean:
        resample 5m notional into 1h summed notional, then take mean
    """
    work_df = df.copy()

    work_df["notional"] = pd.to_numeric(work_df["close"], errors="coerce") * pd.to_numeric(
        work_df["volume"], errors="coerce"
    )

    liq_5min_mean = float(work_df["notional"].tail(1000).mean())

    work_df["dt"] = pd.to_datetime(work_df["ts"], unit="ms", utc=True)
    work_df = work_df.set_index("dt")

    hourly_notional = work_df["notional"].resample("1h").sum()
    liq_1h_mean = float(hourly_notional.mean())

    return liq_5min_mean, liq_1h_mean


class AssetWorker:
    def __init__(self) -> None:
        self.project_root = get_project_root()
        self.worker_dir = Path(__file__).resolve().parent
        self.rules_dir = self.worker_dir / "rules"
        self.sql_dir = self.worker_dir / "sql_queries"

        self.rules = load_rules_file(self.rules_dir / "asset_rules.txt")

        self.batch_size = int(self.rules["BATCH_SIZE"])
        self.timeframe = self.rules["TIMEFRAME"]
        self.target_candles = int(self.rules["TARGET_CANDLES"])
        self.skip_fresh_recheck_days = int(self.rules["SKIP_FRESH_RECHECK_DAYS"])
        self.tested_refresh_days = int(self.rules["TESTED_REFRESH_DAYS"])
        self.adf_alpha = float(self.rules["ADF_ALPHA"])
        self.adf_use_log = str_to_bool(self.rules["ADF_USE_LOG"])
        self.min_ohlcv_rows = int(self.rules["MIN_OHLCV_ROWS"])
        self.bybit_api_file = self.rules["BYBIT_API_FILE"]
        self.mysql_api_file = self.rules["MYSQL_API_FILE"]

        log_file = self.project_root / self.rules["LOG_FILE"]
        self.logger = setup_logger("asset_worker", log_file)

        self.sql_upsert_listed_assets = load_text_file(self.sql_dir / "upsert_listed_assets.txt")
        self.sql_mark_delisted_assets = load_text_file(self.sql_dir / "mark_delisted_assets.txt")
        self.sql_select_assets = load_text_file(self.sql_dir / "select_assets_for_processing.txt")
        self.sql_update_asset_skip_fresh = load_text_file(
            self.sql_dir / "update_asset_skip_fresh.txt"
        )
        self.sql_update_asset_tested = load_text_file(self.sql_dir / "update_asset_tested.txt")

        self.bybit_client = create_bybit_client(self.bybit_api_file)

    def sync_exchange_symbols(self) -> list[str]:
        listed_symbols = fetch_linear_perpetual_symbols(self.bybit_client)

        if not listed_symbols:
            raise RuntimeError("Bybit returned zero listed linear perpetual symbols. Sync aborted.")

        params_seq = [(symbol,) for symbol in listed_symbols]
        execute_many(
            sql=self.sql_upsert_listed_assets,
            api_file_name=self.mysql_api_file,
            params_seq=params_seq,
        )

        placeholders, params = build_not_in_params(listed_symbols)
        mark_delisted_sql = self.sql_mark_delisted_assets.format(
            listed_symbols_placeholders=placeholders
        )
        execute(
            sql=mark_delisted_sql,
            api_file_name=self.mysql_api_file,
            params=params,
        )

        self.logger.info("Market sync completed | listed_symbols=%s", len(listed_symbols))
        return listed_symbols

    def select_assets_for_processing(self) -> list[dict[str, Any]]:
        sql = self.sql_select_assets.format(
            skip_fresh_recheck_days=self.skip_fresh_recheck_days,
            tested_refresh_days=self.tested_refresh_days,
            batch_size=self.batch_size,
        )

        rows = fetch_all(
            sql=sql,
            api_file_name=self.mysql_api_file,
        )

        self.logger.info("Selected assets for processing | count=%s", len(rows))
        return rows

    def mark_skip_fresh(self, symbol: str) -> None:
        execute(
            sql=self.sql_update_asset_skip_fresh,
            api_file_name=self.mysql_api_file,
            params=(symbol,),
        )

    def mark_tested(
        self,
        symbol: str,
        liq_5min_mean: float,
        liq_1h_mean: float,
        adf_stat: float,
        p_value: float,
        is_non_stationary: bool,
    ) -> None:
        execute(
            sql=self.sql_update_asset_tested,
            api_file_name=self.mysql_api_file,
            params=(
                liq_5min_mean,
                liq_1h_mean,
                adf_stat,
                p_value,
                int(is_non_stationary),
                symbol,
            ),
        )

    def process_one_asset(self, symbol: str) -> None:
        self.logger.info("Processing asset | symbol=%s", symbol)

        rows = fetch_ohlcv_with_retry(
            bybit_client=self.bybit_client,
            symbol=symbol,
            timeframe=self.timeframe,
            limit=self.target_candles,
        )

        row_count = len(rows)

        if row_count < self.min_ohlcv_rows:
            self.mark_skip_fresh(symbol)
            self.logger.info(
                "Asset marked skip_fresh | symbol=%s rows=%s target=%s",
                symbol,
                row_count,
                self.min_ohlcv_rows,
            )
            cleanup_objects(rows)
            force_gc()
            return

        df = build_ohlcv_dataframe(rows)
        write_symbol_ohlcv_parquet(symbol, df)

        liq_5min_mean, liq_1h_mean = compute_liquidity_metrics(df)

        adf_result = run_adf_test_from_close_df(
            df=df,
            alpha=self.adf_alpha,
            use_log=self.adf_use_log,
            close_column="close",
        )

        self.mark_tested(
            symbol=symbol,
            liq_5min_mean=liq_5min_mean,
            liq_1h_mean=liq_1h_mean,
            adf_stat=float(adf_result["adf"]),
            p_value=float(adf_result["p_value"]),
            is_non_stationary=bool(adf_result["is_non_stationary"]),
        )

        self.logger.info(
            "Asset tested | symbol=%s rows=%s liq_5min_mean=%.4f liq_1h_mean=%.4f adf=%.6f p_value=%.6f non_stationary=%s",
            symbol,
            row_count,
            liq_5min_mean,
            liq_1h_mean,
            float(adf_result["adf"]),
            float(adf_result["p_value"]),
            bool(adf_result["is_non_stationary"]),
        )

        cleanup_objects(rows, df, adf_result)
        force_gc()

    def run(self) -> None:
        self.logger.info("Asset worker started")

        listed_symbols = self.sync_exchange_symbols()
        self.logger.info("Listed symbols synced | count=%s", len(listed_symbols))

        total_processed = 0
        batch_number = 0

        while True:
            assets = self.select_assets_for_processing()

            if not assets:
                self.logger.info(
                    "No more assets selected for processing | total_processed=%s batches=%s",
                    total_processed,
                    batch_number,
                )
                break

            batch_number += 1
            self.logger.info(
                "Starting batch | batch_number=%s batch_size=%s",
                batch_number,
                len(assets),
            )

            for asset in assets:
                symbol = asset["symbol"]

                try:
                    self.process_one_asset(symbol)
                    total_processed += 1
                except Exception:
                    self.logger.exception("Asset processing failed | symbol=%s", symbol)
                    force_gc()

            force_gc()

        self.logger.info(
            "Asset worker finished | total_processed=%s batches=%s",
            total_processed,
            batch_number,
        )


def main() -> None:
    worker = AssetWorker()
    worker.run()


if __name__ == "__main__":
    main()