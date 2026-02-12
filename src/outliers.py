# src/outliers.py
"""Outlier calculation and filtering utilities.

This version separates:
- metric_direction: how to interpret the metric (is "good" high or low?)
- outlier_side: which tail to select ("best", "worst", or "both" for percentile)

Supported targets:
- outlier_limit_target = "calc": compare against raw calc column
- outlier_limit_target = "percentile": compute percentile rank from calc and compare

Notes:
- outlier_side="both" is supported only when outlier_limit_target="percentile" in this implementation,
  because selecting both tails for raw calc typically needs two thresholds.
"""

from __future__ import annotations

import pandas as pd


def compute_outliers(
    df: pd.DataFrame,
    *,
    outlier_limit_target: str = "percentile",
    outlier_limit: float = 0.95,
    metric_direction: str = "higher_is_good",
    outlier_side: str = "worst",  # "best" | "worst" | "both"
    denominator_limit: int = 1,
    calc_col: str = "calc",
    den_col: str = "den",
) -> pd.DataFrame:
    """
    Compute outliers from a DataFrame that contains at least calc_col and den_col.

    Parameters
    ----------
    outlier_limit_target:
        "percentile" or "calc"
    outlier_limit:
        If target="percentile": percentile cutoff in [0,1] (e.g., 0.90)
        If target="calc": raw cutoff value (e.g., 0.25)
    metric_direction:
        "higher_is_good" or "lower_is_good" (interpretation of what performance means)
    outlier_side:
        "best": select best performers (tail of "good")
        "worst": select worst performers (tail opposite of "good")
        "both": select both tails (percentile only; uses outlier_limit and 1-outlier_limit)
    denominator_limit:
        Minimum den_col value to include in consideration.
    calc_col / den_col:
        Column names in df.
    """
    if calc_col not in df.columns or den_col not in df.columns:
        raise KeyError(f"DataFrame must contain columns: '{calc_col}' and '{den_col}'")

    df2 = df.copy()
    df2 = df2[df2[den_col] >= denominator_limit].copy()
    if df2.empty:
        return df2.reset_index(drop=True)

    direction = str(metric_direction).lower()
    side = str(outlier_side).lower()
    target = str(outlier_limit_target).lower()

    if direction not in ("higher_is_good", "lower_is_good"):
        raise ValueError("metric_direction must be 'higher_is_good' or 'lower_is_good'")
    if side not in ("best", "worst", "both"):
        raise ValueError("outlier_side must be 'best', 'worst', or 'both'")
    if target not in ("percentile", "calc"):
        raise ValueError("outlier_limit_target must be 'percentile' or 'calc'")

    higher_is_good = direction == "higher_is_good"

    # Build the series we compare against the outlier_limit
    if target == "percentile":
        df2["percentile"] = df2[calc_col].rank(pct=True, method="average")
        value = df2["percentile"]
        sort_col = "percentile"
    else:
        value = df2[calc_col]
        sort_col = calc_col

    # Determine comparisons for best/worst based on metric interpretation
    # - higher_is_good: best = high values, worst = low values
    # - lower_is_good:  best = low values,  worst = high values
    if side == "best":
        mask = (value >= outlier_limit) if higher_is_good else (value <= outlier_limit)
        sort_ascending = False if higher_is_good else True

    elif side == "worst":
        mask = (value <= outlier_limit) if higher_is_good else (value >= outlier_limit)
        sort_ascending = True if higher_is_good else False

    else:  # side == "both"
        if target != "percentile":
            raise ValueError("outlier_side='both' is only supported when outlier_limit_target='percentile'.")

        low_cut = 1.0 - float(outlier_limit)
        high_cut = float(outlier_limit)

        # For percentiles, "both" tails are symmetric regardless of direction
        mask = (value <= low_cut) | (value >= high_cut)

        # Sort with most extreme at top: high percentiles first, then low
        # (You can change this behavior if you prefer low tail first.)
        df_out = df2[mask].copy()
        df_out = df_out.sort_values(sort_col, ascending=False)
        return df_out.reset_index(drop=True)

    df_out = df2[mask].copy().sort_values(sort_col, ascending=sort_ascending)
    return df_out.reset_index(drop=True)
