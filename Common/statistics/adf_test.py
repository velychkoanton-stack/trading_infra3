import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller


def run_adf_test_from_series(
    series: pd.Series,
    alpha: float = 0.05,
    use_log: bool = True,
) -> dict[str, float | bool]:
    """
    Run ADF test on a pandas Series.

    Returns:
    {
        "adf": float,
        "p_value": float,
        "is_non_stationary": bool
    }

    Notes:
    - is_non_stationary = (p_value > alpha)
    - By default log transform is used for price stability
    """
    clean_series = pd.to_numeric(series, errors="coerce").dropna()

    if clean_series.empty:
        raise ValueError("ADF input series is empty after cleaning.")

    if len(clean_series) < 20:
        raise ValueError(f"ADF input series is too short: len={len(clean_series)}")

    if use_log:
        if (clean_series <= 0).any():
            raise ValueError("Cannot apply log transform: series contains non-positive values.")
        test_series = np.log(clean_series)
    else:
        test_series = clean_series

    result = adfuller(test_series)

    adf_stat = float(result[0])
    p_value = float(result[1])
    is_non_stationary = bool(p_value > alpha)

    return {
        "adf": adf_stat,
        "p_value": p_value,
        "is_non_stationary": is_non_stationary,
    }


def run_adf_test_from_close_df(
    df: pd.DataFrame,
    alpha: float = 0.05,
    use_log: bool = True,
    close_column: str = "close",
) -> dict[str, float | bool]:
    """
    Run ADF test from a standard OHLCV DataFrame using close column.
    """
    if close_column not in df.columns:
        raise ValueError(f"Close column not found: {close_column}")

    return run_adf_test_from_series(
        series=df[close_column],
        alpha=alpha,
        use_log=use_log,
    )