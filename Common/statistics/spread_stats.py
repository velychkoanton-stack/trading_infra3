import pandas as pd


def clean_spread_series(series: pd.Series) -> pd.Series:
    """
    Convert to numeric and drop NaN values.
    """
    clean = pd.to_numeric(series, errors="coerce").dropna()

    if clean.empty:
        raise ValueError("Spread series is empty after cleaning.")

    return clean.astype(float)


def calculate_spread_skew(series: pd.Series) -> float:
    """
    Sample skewness of spread series.
    """
    clean = clean_spread_series(series)
    return float(clean.skew())


def calculate_spread_kurt(series: pd.Series) -> float:
    """
    Excess kurtosis of spread series.
    """
    clean = clean_spread_series(series)
    return float(clean.kurt())


def calculate_spread_mean(series: pd.Series) -> float:
    clean = clean_spread_series(series)
    return float(clean.mean())


def calculate_spread_std(series: pd.Series) -> float:
    clean = clean_spread_series(series)
    return float(clean.std())


def calculate_spread_stats(series: pd.Series) -> dict[str, float]:
    """
    Compact helper for common spread distribution stats.
    """
    clean = clean_spread_series(series)

    return {
        "spread_mean": float(clean.mean()),
        "spread_std": float(clean.std()),
        "spread_skew": float(clean.skew()),
        "spread_kurt": float(clean.kurt()),
        "spread_min": float(clean.min()),
        "spread_max": float(clean.max()),
    }