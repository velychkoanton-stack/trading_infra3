# =========================
# lib/db.py
# =========================

import os
import logging
import mysql.connector
from contextlib import contextmanager

logger = logging.getLogger(__name__)


def get_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        autocommit=True
    )


@contextmanager
def get_cursor():
    conn = get_connection()
    try:
        cursor = conn.cursor(dictionary=True)
        yield cursor
    finally:
        cursor.close()
        conn.close()


def execute(query, params=None):
    with get_cursor() as cur:
        cur.execute(query, params or ())
        return cur.rowcount


def fetch_all(query, params=None):
    with get_cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchall()


def fetch_one(query, params=None):
    with get_cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchone()


def executemany(query, rows):
    with get_cursor() as cur:
        cur.executemany(query, rows)
        return cur.rowcount


# =========================
# lib/bybit.py
# =========================

import os
import time
import logging
import ccxt

logger = logging.getLogger(__name__)


def make_bybit():

    exchange = ccxt.bybit({
        "apiKey": os.getenv("BYBIT_API_KEY"),
        "secret": os.getenv("BYBIT_API_SECRET"),
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap"
        }
    })

    return exchange


def fetch_linear_perp_symbols(bybit):

    markets = bybit.load_markets()

    symbols = []

    for symbol, m in markets.items():
        if (
            m.get("swap") is True
            and m.get("linear") is True
            and m.get("active") is True
        ):
            symbols.append(symbol)

    return sorted(symbols)


def retry(func, max_attempts=5):

    def wrapper(*args, **kwargs):

        delay = 1

        for attempt in range(max_attempts):
            try:
                return func(*args, **kwargs)
            except (ccxt.NetworkError, ccxt.ExchangeError) as e:

                logger.warning(f"Retry {attempt+1}/{max_attempts} after error: {e}")

                if attempt == max_attempts - 1:
                    raise

                time.sleep(delay)
                delay *= 2

    return wrapper


@retry
def fetch_ohlcv_10k_simple(bybit, symbol):

    rows = bybit.fetch_ohlcv(
        symbol,
        timeframe="5m",
        limit=10000,
        params={"paginate": True}
    )

    return rows


# =========================
# lib/parquet_store.py
# =========================

import os
import pandas as pd

DATA_DIR = "./data/parquet/bybit_linear_5m"


def sanitize_symbol_to_filename(symbol: str):

    s = symbol.replace("/", "_").replace(":", "_")
    return f"{s}.parquet"


def save_parquet(symbol, df: pd.DataFrame):

    os.makedirs(DATA_DIR, exist_ok=True)

    filename = sanitize_symbol_to_filename(symbol)

    path = os.path.join(DATA_DIR, filename)

    df.to_parquet(path, engine="pyarrow", index=False)

    return path


# =========================
# lib/stats.py
# =========================

import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller


def compute_liquidity_metrics(df: pd.DataFrame):

    # Notional per 5m bar
    notional = df["close"] * df["volume"]

    liq_5m_mean = float(notional.tail(1000).mean())

    df_tmp = df.copy()
    df_tmp["notional"] = notional
    df_tmp["ts"] = pd.to_datetime(df_tmp["ts"], unit="ms")

    df_tmp = df_tmp.set_index("ts")

    # Aggregate to hourly notional
    hourly = df_tmp["notional"].resample("1H").sum()

    liq_1h_mean = float(hourly.mean())

    return liq_5m_mean, liq_1h_mean


def compute_adf(df: pd.DataFrame):

    # Use log-price for stability
    log_close = np.log(df["close"])

    result = adfuller(log_close)

    adf_stat = float(result[0])
    p_value = float(result[1])

    # Non-stationary if p-value > 0.05
    non_stationary = p_value > 0.05

    return adf_stat, p_value, non_stationary


# =========================
# workers/asset_worker.py
# =========================

import os
import logging
from datetime import datetime

import pandas as pd

from lib.db import execute, fetch_all
from lib.bybit import make_bybit, fetch_linear_perp_symbols, fetch_ohlcv_10k_simple
from lib.parquet_store import save_parquet
from lib.stats import compute_liquidity_metrics, compute_adf


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("asset_worker")


BATCH_SIZE = 20


def sync_markets(bybit):

    logger.info("Loading markets from Bybit")

    symbols = fetch_linear_perp_symbols(bybit)

    logger.info(f"{len(symbols)} active linear perps detected")

    # Insert/update listed markets
    rows = [(s,) for s in symbols]

    query = """
    INSERT INTO asset_universe (symbol, exchange_status, pipe_status, created_at, updated_at)
    VALUES (%s, 'listed', 'new', NOW(), NOW())
    ON DUPLICATE KEY UPDATE
        exchange_status='listed',
        updated_at=NOW()
    """

    execute_many_safe(query, rows)

    # Mark delisted
    query = """
    UPDATE asset_universe
    SET exchange_status='delisted', updated_at=NOW()
    WHERE symbol NOT IN (%s)
    """

    if symbols:
        placeholders = ",".join(["%s"] * len(symbols))
        query = f"""
        UPDATE asset_universe
        SET exchange_status='delisted', updated_at=NOW()
        WHERE symbol NOT IN ({placeholders})
        """

        execute(query, symbols)


def execute_many_safe(query, rows):

    from lib.db import executemany

    executemany(query, rows)


def select_assets_for_processing():

    query = f"""
    SELECT symbol
    FROM asset_universe
    WHERE exchange_status='listed'
      AND (
            pipe_status='new'
         OR (pipe_status='skip_fresh' AND last_update_ts < NOW() - INTERVAL 14 DAY)
         OR (pipe_status='tested' AND last_update_ts < NOW() - INTERVAL 7 DAY)
      )
    ORDER BY last_update_ts IS NULL DESC, last_update_ts ASC
    LIMIT {BATCH_SIZE}
    """

    rows = fetch_all(query)

    return [r["symbol"] for r in rows]


def mark_skip(symbol):

    query = """
    UPDATE asset_universe
    SET pipe_status='skip_fresh',
        last_update_ts=NOW(),
        updated_at=NOW()
    WHERE symbol=%s
    """

    execute(query, (symbol,))


def mark_tested(symbol, liq5, liq1h, adf, pval, nonstat):

    query = """
    UPDATE asset_universe
    SET pipe_status='tested',
        liq_5min_mean=%s,
        liq_1h_mean=%s,
        adf=%s,
        p_value=%s,
        non_stationary_status=%s,
        last_update_ts=NOW(),
        updated_at=NOW()
    WHERE symbol=%s
    """

    execute(query, (liq5, liq1h, adf, pval, nonstat, symbol))


def process_symbol(bybit, symbol):

    logger.info(f"Processing {symbol}")

    rows = fetch_ohlcv_10k_simple(bybit, symbol)

    if len(rows) < 10000:

        logger.info(f"{symbol} skipped (fresh listing: {len(rows)} candles)")

        mark_skip(symbol)

        return

    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])

    save_parquet(symbol, df)

    liq5, liq1h = compute_liquidity_metrics(df)

    adf, pval, nonstat = compute_adf(df)

    mark_tested(symbol, liq5, liq1h, adf, pval, nonstat)

    logger.info(
        f"{symbol} tested | liq5={liq5:.2f} liq1h={liq1h:.2f} p={pval:.4f}"
    )


def main():
    logger.info("Asset worker started")
    bybit = make_bybit()
    sync_markets(bybit)
    assets = select_assets_for_processing()
    logger.info(f"{len(assets)} assets selected")
    for symbol in assets:
        try:
            process_symbol(bybit, symbol)
        except Exception as e:
            logger.exception(f"Failed processing {symbol}: {e}")


if __name__ == "__main__":
    main()