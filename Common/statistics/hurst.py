import numpy as np
import pandas as pd


def calculate_hurst_exponent(
    series: pd.Series,
    min_lag: int = 2,
    max_lag: int = 100,
) -> float:
    """
    Estimate Hurst exponent using log-log fit of lagged std dev.

    Interpretation:
    - H < 0.5  : mean-reverting
    - H ~= 0.5 : random walk
    - H > 0.5  : trending

    Notes:
    - This is a practical estimator, not a perfect one
    - Keep deterministic and dependency-light
    """
    values = pd.to_numeric(series, errors="coerce").dropna().astype(float).to_numpy()

    if len(values) < max_lag + 5:
        raise ValueError(
            f"Series too short for Hurst estimation: len={len(values)}, required>{max_lag + 5}"
        )

    lags = range(min_lag, max_lag + 1)
    tau_values = []

    for lag in lags:
        diff = values[lag:] - values[:-lag]
        std = np.std(diff)

        if std <= 0 or np.isnan(std) or np.isinf(std):
            continue

        tau_values.append((lag, std))

    if len(tau_values) < 10:
        raise ValueError("Not enough valid lag observations to estimate Hurst exponent.")

    log_lags = np.log([item[0] for item in tau_values])
    log_tau = np.log([item[1] for item in tau_values])

    slope, _intercept = np.polyfit(log_lags, log_tau, 1)

    hurst = float(slope)

    if np.isnan(hurst) or np.isinf(hurst):
        raise ValueError(f"Invalid Hurst result: {hurst}")

    return hurst