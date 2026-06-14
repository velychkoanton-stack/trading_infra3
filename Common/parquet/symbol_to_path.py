from pathlib import Path

from Common.config.path_config import get_project_root


def sanitize_symbol(symbol: str) -> str:
    """
    Convert exchange symbol to filesystem-safe parquet filename stem.

    Examples:
    - BTC/USDT:USDT -> BTC_USDT_USDT
    - ETH/USDC:USDC -> ETH_USDC_USDC
    """
    sanitized = symbol.strip()
    sanitized = sanitized.replace("/", "_")
    sanitized = sanitized.replace(":", "_")
    sanitized = sanitized.replace("\\", "_")
    sanitized = sanitized.replace(" ", "_")
    return sanitized


def get_bybit_linear_5m_dir() -> Path:
    """
    Returns:
    Trading_infra/data/parquet_db/bybit_linear_5m/
    """
    return get_project_root() / "data" / "parquet_db" / "bybit_linear_5m"


def get_bybit_linear_timeframe_dir(storage_name: str) -> Path:
    clean_name = str(storage_name).strip()
    if not clean_name or clean_name in {".", ".."}:
        raise ValueError(f"Invalid parquet storage name: {storage_name!r}")
    if "/" in clean_name or "\\" in clean_name:
        raise ValueError("Parquet storage name must be a directory name, not a path")
    return get_project_root() / "data" / "parquet_db" / clean_name


def get_symbol_parquet_path(symbol: str) -> Path:
    """
    Build full parquet path for one symbol.
    """
    file_name = f"{sanitize_symbol(symbol)}.parquet"
    return get_bybit_linear_5m_dir() / file_name


def get_symbol_parquet_path_for_storage(symbol: str, storage_name: str) -> Path:
    file_name = f"{sanitize_symbol(symbol)}.parquet"
    return get_bybit_linear_timeframe_dir(storage_name) / file_name
