import numpy as np
import pandas as pd


def align_close_series(
    df_1: pd.DataFrame,
    df_2: pd.DataFrame,
    ts_column: str = "ts",
    close_column: str = "close",
) -> pd.DataFrame:
    """
    Align two OHLCV dataframes by timestamp and return a compact dataframe:

    columns:
    - ts
    - close_1
    - close_2
    """
    required_cols = {ts_column, close_column}

    if not required_cols.issubset(df_1.columns):
        raise ValueError(f"df_1 missing required columns: {required_cols}")
    if not required_cols.issubset(df_2.columns):
        raise ValueError(f"df_2 missing required columns: {required_cols}")

    left = df_1[[ts_column, close_column]].copy()
    left.columns = ["ts", "close_1"]

    right = df_2[[ts_column, close_column]].copy()
    right.columns = ["ts", "close_2"]

    merged = pd.merge(left, right, on="ts", how="inner")
    merged["close_1"] = pd.to_numeric(merged["close_1"], errors="coerce")
    merged["close_2"] = pd.to_numeric(merged["close_2"], errors="coerce")
    merged = merged.dropna(subset=["close_1", "close_2"]).sort_values("ts").reset_index(drop=True)

    return merged


def calculate_beta_ols(
    price_1: pd.Series,
    price_2: pd.Series,
    use_log: bool = True,
) -> float:
    """
    Calculate hedge ratio (beta) by simple OLS slope without external deps.

    Model:
        y = a + b*x

    Where:
        y = asset_1
        x = asset_2

    Returned beta = b

    Notes:
    - If use_log=True, regression is performed on log-prices
    - This is usually the practical default for pair trading
    """
    y = pd.to_numeric(price_1, errors="coerce").dropna()
    x = pd.to_numeric(price_2, errors="coerce").dropna()

    aligned = pd.concat([y, x], axis=1).dropna()
    if aligned.empty or len(aligned) < 30:
        raise ValueError(f"Not enough aligned observations for beta calculation: len={len(aligned)}")

    y_arr = aligned.iloc[:, 0].astype(float).to_numpy()
    x_arr = aligned.iloc[:, 1].astype(float).to_numpy()

    if use_log:
        if (y_arr <= 0).any() or (x_arr <= 0).any():
            raise ValueError("Cannot calculate log-beta: non-positive values detected.")
        y_arr = np.log(y_arr)
        x_arr = np.log(x_arr)

    x_mean = x_arr.mean()
    y_mean = y_arr.mean()

    denominator = np.sum((x_arr - x_mean) ** 2)
    if denominator == 0:
        raise ValueError("Beta denominator is zero. Asset_2 has no variance.")

    numerator = np.sum((x_arr - x_mean) * (y_arr - y_mean))
    beta = numerator / denominator

    return float(beta)


def calculate_beta_from_dfs(
    df_1: pd.DataFrame,
    df_2: pd.DataFrame,
    ts_column: str = "ts",
    close_column: str = "close",
    use_log: bool = True,
) -> float:
    """
    Align two OHLCV dataframes and calculate beta.
    """
    aligned = align_close_series(
        df_1=df_1,
        df_2=df_2,
        ts_column=ts_column,
        close_column=close_column,
    )

    return calculate_beta_ols(
        price_1=aligned["close_1"],
        price_2=aligned["close_2"],
        use_log=use_log,
    )


def normalize_beta(beta: float) -> float:
    """
    Return absolute beta-normalized ratio for sizing logic.

    Practical interpretation:
    - beta_norm = abs(beta)
    - if beta is invalid or too small, caller should reject or clamp upstream

    We keep this function simple and deterministic.
    """
    beta_value = float(beta)

    if np.isnan(beta_value) or np.isinf(beta_value):
        raise ValueError(f"Invalid beta value: {beta_value}")

    return abs(beta_value)


def build_spread_from_beta(
    price_1: pd.Series,
    price_2: pd.Series,
    beta: float,
    use_log: bool = True,
) -> pd.Series:
    """
    Build spread:

    if use_log:
        spread = log(price_1) - beta * log(price_2)
    else:
        spread = price_1 - beta * price_2
    """
    p1 = pd.to_numeric(price_1, errors="coerce")
    p2 = pd.to_numeric(price_2, errors="coerce")

    spread_df = pd.concat([p1, p2], axis=1).dropna()
    if spread_df.empty:
        raise ValueError("Cannot build spread: no aligned price values.")

    p1_arr = spread_df.iloc[:, 0].astype(float)
    p2_arr = spread_df.iloc[:, 1].astype(float)

    if use_log:
        if (p1_arr <= 0).any() or (p2_arr <= 0).any():
            raise ValueError("Cannot build log-spread: non-positive values detected.")
        spread = np.log(p1_arr) - float(beta) * np.log(p2_arr)
    else:
        spread = p1_arr - float(beta) * p2_arr

    return pd.Series(spread, index=spread_df.index, name="spread")


def build_spread_from_dfs(
    df_1: pd.DataFrame,
    df_2: pd.DataFrame,
    beta: float,
    ts_column: str = "ts",
    close_column: str = "close",
    use_log: bool = True,
) -> pd.DataFrame:
    """
    Align two OHLCV dataframes and return:

    columns:
    - ts
    - close_1
    - close_2
    - spread
    """
    aligned = align_close_series(
        df_1=df_1,
        df_2=df_2,
        ts_column=ts_column,
        close_column=close_column,
    )

    spread = build_spread_from_beta(
        price_1=aligned["close_1"],
        price_2=aligned["close_2"],
        beta=beta,
        use_log=use_log,
    )

    result = aligned.copy()
    result["spread"] = spread.to_numpy()

    return result