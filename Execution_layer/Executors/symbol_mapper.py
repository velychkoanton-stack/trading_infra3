from __future__ import annotations


def ccxt_symbol_to_asset(symbol: str) -> str:
    """
    BTC/USDT:USDT -> BTC
    ETH/USDT:USDT -> ETH
    """
    if not symbol or "/" not in symbol:
        raise ValueError(f"Invalid CCXT symbol: {symbol}")

    return symbol.split("/", 1)[0].strip().upper()


def pybit_symbol_to_asset(symbol: str) -> str:
    """
    BTCUSDT -> BTC
    ETHUSDT -> ETH
    """
    if not symbol:
        raise ValueError(f"Invalid pybit symbol: {symbol}")

    normalized = symbol.strip().upper()

    if normalized.endswith("USDT"):
        return normalized[:-4]

    raise ValueError(f"Unsupported pybit symbol format: {symbol}")


def ccxt_symbol_to_pybit_symbol(symbol: str) -> str:
    """
    BTC/USDT:USDT -> BTCUSDT
    """
    asset = ccxt_symbol_to_asset(symbol)
    return f"{asset}USDT"


def assets_to_uuid(asset_1: str, asset_2: str) -> str:
    """
    BTC, ETH -> BTC_ETH
    """
    a1 = str(asset_1).strip().upper()
    a2 = str(asset_2).strip().upper()

    if not a1 or not a2:
        raise ValueError("asset_1 and asset_2 must not be empty")

    return f"{a1}_{a2}"


def uuid_to_assets(uuid: str) -> tuple[str, str]:
    """
    BTC_ETH -> (BTC, ETH)
    """
    if not uuid or "_" not in uuid:
        raise ValueError(f"Invalid uuid format: {uuid}")

    left, right = uuid.split("_", 1)
    left = left.strip().upper()
    right = right.strip().upper()

    if not left or not right:
        raise ValueError(f"Invalid uuid format: {uuid}")

    return left, right