from pathlib import Path

import pandas as pd

from Common.parquet.symbol_to_path import (
    get_symbol_parquet_path,
    get_symbol_parquet_path_for_storage,
)


def parquet_exists(symbol: str) -> bool:
    path = get_symbol_parquet_path(symbol)
    return path.exists()


def read_symbol_ohlcv_parquet(symbol: str) -> pd.DataFrame:
    """
    Read one symbol parquet file into DataFrame.

    Raises:
    - FileNotFoundError if parquet does not exist
    """
    path: Path = get_symbol_parquet_path(symbol)

    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found for symbol={symbol}: {path}")

    return pd.read_parquet(path, engine="pyarrow")


def parquet_exists_in_storage(symbol: str, storage_name: str) -> bool:
    return get_symbol_parquet_path_for_storage(symbol, storage_name).exists()


def read_symbol_ohlcv_parquet_from_storage(
    symbol: str,
    storage_name: str,
) -> pd.DataFrame:
    path = get_symbol_parquet_path_for_storage(symbol, storage_name)
    if not path.exists():
        raise FileNotFoundError(
            f"Parquet file not found for symbol={symbol}, storage={storage_name}: {path}"
        )
    return pd.read_parquet(path, engine="pyarrow")
