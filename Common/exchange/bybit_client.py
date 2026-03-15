import time
from pathlib import Path
from typing import Any

import ccxt

from Common.config.api_loader import load_api_file
from Common.config.path_config import get_api_file_path


RETRYABLE_EXCEPTIONS = (
    ccxt.NetworkError,
    ccxt.RequestTimeout,
    ccxt.ExchangeNotAvailable,
    ccxt.DDoSProtection,
    ccxt.RateLimitExceeded,
)


def create_bybit_client(api_file_name: str, demo: bool = False) -> ccxt.bybit:
    """
    Create Bybit CCXT client from API txt file.

    Expected API file format:
    API_KEY=...
    API_SECRET=...

    Args:
        api_file_name: file name resolved via get_api_file_path()
        demo: if True, enable Bybit demo trading mode
    """
    api_path: Path = get_api_file_path(api_file_name)
    api_config = load_api_file(api_path)

    api_key = api_config.get("API_KEY", "")
    api_secret = api_config.get("API_SECRET", "")

    client = ccxt.bybit(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": "swap",
            },
            "timeout": 20000,
        }
    )

    if demo:
        if hasattr(client, "enable_demo_trading"):
            try:
                client.enable_demo_trading(True)
            except Exception:
                pass

    return client


def fetch_linear_perpetual_symbols(bybit_client: ccxt.bybit) -> list[str]:
    """
    Load markets and return active USDT-margined linear perpetual swap symbols.

    Filter:
    - swap=True
    - linear=True
    - active=True
    """
    markets = bybit_client.load_markets()

    symbols: list[str] = []

    for symbol, market in markets.items():
        if (
            market.get("swap") is True
            and market.get("linear") is True
            and market.get("active") is True
        ):
            symbols.append(symbol)

    symbols.sort()
    return symbols


def fetch_ohlcv_with_retry(
    bybit_client: ccxt.bybit,
    symbol: str,
    timeframe: str,
    limit: int,
    max_retries: int = 5,
    initial_sleep_seconds: float = 1.0,
) -> list[list[Any]]:
    """
    Fetch OHLCV with exponential backoff for retryable CCXT errors.

    Uses the call that is already verified on your server:
    bybit.fetch_ohlcv(symbol, timeframe="5m", limit=10000, params={"paginate": True})
    """
    attempt = 0
    sleep_seconds = initial_sleep_seconds

    while True:
        try:
            rows = bybit_client.fetch_ohlcv(
                symbol,
                timeframe=timeframe,
                limit=limit,
                params={"paginate": True},
            )
            return rows

        except RETRYABLE_EXCEPTIONS:
            attempt += 1

            if attempt > max_retries:
                raise

            time.sleep(sleep_seconds)
            sleep_seconds *= 2


def build_not_in_params(values: list[str]) -> tuple[str, tuple[Any, ...]]:
    """
    Helper for SQL:
    returns placeholders string and params tuple.

    Example:
    ['BTC/USDT:USDT', 'ETH/USDT:USDT']
    ->
    ("%s,%s", ('BTC/USDT:USDT', 'ETH/USDT:USDT'))
    """
    if not values:
        raise ValueError("values cannot be empty for NOT IN placeholder building")

    placeholders = ",".join(["%s"] * len(values))
    return placeholders, tuple(values)