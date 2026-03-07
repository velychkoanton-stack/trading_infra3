def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(float(value), max_value))


def scale_high_good(value: float, good_min: float, good_max: float) -> float:
    """
    Higher is better.
    Returns score in [0, 100].
    """
    if good_max <= good_min:
        raise ValueError("good_max must be greater than good_min")

    normalized = (float(value) - good_min) / (good_max - good_min)
    return clamp(normalized * 100.0, 0.0, 100.0)


def scale_low_good(value: float, good_min: float, good_max: float) -> float:
    """
    Lower is better.
    Returns score in [0, 100].

    Example:
    lower p-value -> better
    lower half-life -> better (within reason)
    """
    if good_max <= good_min:
        raise ValueError("good_max must be greater than good_min")

    normalized = (good_max - float(value)) / (good_max - good_min)
    return clamp(normalized * 100.0, 0.0, 100.0)


def score_adf_pvalue(p_value: float) -> float:
    """
    Lower p-value is better for spread stationarity.
    Practical scoring window:
    - p <= 0.01  -> near max score
    - p >= 0.10  -> near min score
    """
    return scale_low_good(value=p_value, good_min=0.01, good_max=0.10)


def score_hurst(hurst: float) -> float:
    """
    Lower Hurst is better for mean reversion.
    Practical scoring window:
    - H <= 0.20 -> strong score
    - H >= 0.60 -> weak score
    """
    return scale_low_good(value=hurst, good_min=0.20, good_max=0.60)


def score_half_life(half_life: float) -> float:
    """
    Lower half-life is usually better, but not zero.
    Practical window:
    - HL <= 12  -> strong
    - HL >= 200 -> weak
    """
    return scale_low_good(value=half_life, good_min=12.0, good_max=200.0)


def score_stat_test(
    p_value: float,
    hurst: float,
    half_life: float,
    weight_adf: float = 0.45,
    weight_hurst: float = 0.30,
    weight_half_life: float = 0.25,
) -> float:
    """
    Composite stat score in [0, 100].

    Default weights emphasize spread stationarity first.
    """
    total_weight = weight_adf + weight_hurst + weight_half_life
    if total_weight <= 0:
        raise ValueError("Total score weight must be positive.")

    adf_score = score_adf_pvalue(p_value)
    hurst_score = score_hurst(hurst)
    hl_score = score_half_life(half_life)

    final_score = (
        adf_score * weight_adf
        + hurst_score * weight_hurst
        + hl_score * weight_half_life
    ) / total_weight

    return round(clamp(final_score, 0.0, 100.0), 4)