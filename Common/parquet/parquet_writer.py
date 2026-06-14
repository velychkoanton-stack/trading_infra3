from pathlib import Path

import pandas as pd

from Common.parquet.symbol_to_path import (
    get_symbol_parquet_path,
    get_symbol_parquet_path_for_storage,
)


EXPECTED_COLUMNS = ["ts", "open", "high", "low", "close", "volume"]


def ensure_ohlcv_columns(df: pd.DataFrame) -> None:
    missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"DataFrame is missing required OHLCV columns: {missing}")


def write_symbol_ohlcv_parquet(symbol: str, df: pd.DataFrame) -> Path:
    """
    Save OHLCV dataframe to parquet.

    Expected columns:
    ts, open, high, low, close, volume
    """
    ensure_ohlcv_columns(df)

    path = get_symbol_parquet_path(symbol)
    path.parent.mkdir(parents=True, exist_ok=True)

    df_to_save = df.copy()
    df_to_save.to_parquet(path, engine="pyarrow", index=False)

    return path


def write_symbol_ohlcv_parquet_to_storage(
    symbol: str,
    df: pd.DataFrame,
    storage_name: str,
) -> Path:
    ensure_ohlcv_columns(df)
    path = get_symbol_parquet_path_for_storage(symbol, storage_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.copy().to_parquet(path, engine="pyarrow", index=False)
    return path
