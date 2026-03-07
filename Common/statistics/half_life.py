import numpy as np
import pandas as pd


def calculate_half_life(series: pd.Series) -> float:
    """
    Estimate mean-reversion half-life from spread series.

    Approach:
    Regress delta_spread on lagged_spread:

        delta_s = a + b * s[t-1]

    Then:
        half_life = -ln(2) / b

    Notes:
    - Valid only when b < 0 (mean-reverting)
    - If b >= 0, half-life is treated as infinite / invalid
    """
    spread = pd.to_numeric(series, errors="coerce").dropna().astype(float)

    if len(spread) < 30:
        raise ValueError(f"Spread series too short for half-life calculation: len={len(spread)}")

    lagged = spread.shift(1)
    delta = spread - lagged

    reg_df = pd.DataFrame(
        {
            "lagged": lagged,
            "delta": delta,
        }
    ).dropna()

    if len(reg_df) < 20:
        raise ValueError(f"Not enough observations after lagging for half-life: len={len(reg_df)}")

    x = reg_df["lagged"].to_numpy()
    y = reg_df["delta"].to_numpy()

    x_mean = x.mean()
    y_mean = y.mean()

    denominator = np.sum((x - x_mean) ** 2)
    if denominator == 0:
        raise ValueError("Half-life denominator is zero. Lagged spread variance is zero.")

    slope = np.sum((x - x_mean) * (y - y_mean)) / denominator

    if slope >= 0:
        raise ValueError(
            f"Spread does not appear mean-reverting. Half-life undefined for slope={slope:.6f}"
        )

    half_life = -np.log(2.0) / slope

    if np.isnan(half_life) or np.isinf(half_life) or half_life <= 0:
        raise ValueError(f"Invalid half-life result: {half_life}")

    return float(half_life)