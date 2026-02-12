# src/utils.py
"""
Utility helpers for the outlier pipeline.

This module supports the new 4-stage pipeline structure:

1) Validity filters
   - Statistical reliability guards (e.g., denominator thresholds)

2) Eligibility filters
   - Business/operational gates (e.g., site exclusions, talk time thresholds)

Notes
-----
- Site exclusions support substring matching (case-insensitive) so you can exclude
  "trn" anywhere in the site string.
- Generic eligibility filters support simple operators on arbitrary columns.
"""

from __future__ import annotations

import operator
from typing import Any, Dict, List, Optional

import pandas as pd

# Supported operators for generic eligibility filters
_OPS = {
    "==": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
}


def apply_validity_filters(
    df: pd.DataFrame,
    *,
    denominator_limit: Optional[int] = None,
    den_col: str = "den",
) -> pd.DataFrame:
    """
    Stage 1: Validity filters (statistical reliability).

    Parameters
    ----------
    denominator_limit:
        If provided, keeps rows where df[den_col] >= denominator_limit.
    den_col:
        Column name representing denominator / exposure count.

    Returns
    -------
    Filtered DataFrame copy.
    """
    out = df.copy()

    if denominator_limit is not None:
        if den_col not in out.columns:
            raise KeyError(f"Validity filter requires '{den_col}' column (for denominator_limit).")
        out = out[out[den_col] >= int(denominator_limit)].copy()

    return out


def apply_site_exclude_contains(
    df: pd.DataFrame,
    *,
    site_exclude_contains: Optional[List[str]] = None,
    site_col: str = "site",
) -> pd.DataFrame:
    """
    Eligibility helper: Exclude rows where `site_col` contains any substring in `site_exclude_contains`.

    This is case-insensitive and treats site values as strings. NaNs are treated as not matching.

    Example
    -------
    site_exclude_contains=["trn"] will exclude:
      - "TRN"
      - "east_trn_01"
      - "01-TRN-site"

    Returns
    -------
    Filtered DataFrame copy.
    """
    out = df.copy()
    if not site_exclude_contains:
        return out

    if site_col not in out.columns:
        raise KeyError(f"Eligibility filter requires '{site_col}' column for site exclusions.")

    s = out[site_col].astype(str)
    for needle in site_exclude_contains:
        # recompute s each iteration based on current out to preserve alignment
        s = out[site_col].astype(str)
        out = out[~s.str.contains(str(needle), case=False, na=False)].copy()

    return out


def apply_generic_filters(
    df: pd.DataFrame,
    *,
    filters: Optional[List[Dict[str, Any]]] = None,
) -> pd.DataFrame:
    """
    Eligibility helper: Apply a list of simple column filters.

    Each filter dict should look like:
      - column: <colname>
      - op: one of ["==","!=",">",">=","<","<="]
      - value: <value>

    Example
    -------
    filters:
      - column: talk_time_seconds
        op: ">="
        value: 2000

    Returns
    -------
    Filtered DataFrame copy.
    """
    out = df.copy()
    if not filters:
        return out

    for f in filters:
        if not isinstance(f, dict):
            raise ValueError("Each eligibility filter must be a dict with keys: column, op, value")

        col = f.get("column")
        op = f.get("op")
        val = f.get("value")

        if not col or op is None:
            raise ValueError(f"Invalid filter definition: {f}")

        if col not in out.columns:
            raise KeyError(f"Eligibility filter references missing column: {col}")

        if op not in _OPS:
            raise ValueError(f"Unsupported operator '{op}'. Supported: {sorted(_OPS.keys())}")

        out = out[_OPS[op](out[col], val)].copy()

    return out


def apply_eligibility_filters(
    df: pd.DataFrame,
    *,
    site_exclude_contains: Optional[List[str]] = None,
    filters: Optional[List[Dict[str, Any]]] = None,
    site_col: str = "site",
) -> pd.DataFrame:
    """
    Stage 2: Eligibility filters (business/operational gates).

    Combines:
      - site substring exclusions (site_exclude_contains)
      - generic column filters (filters)

    Returns
    -------
    Filtered DataFrame copy.
    """
    out = df.copy()
    out = apply_site_exclude_contains(out, site_exclude_contains=site_exclude_contains, site_col=site_col)
    out = apply_generic_filters(out, filters=filters)
    return out
