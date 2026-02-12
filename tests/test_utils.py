import pandas as pd
import pytest

from src.utils import apply_validity_filters, apply_eligibility_filters


def test_apply_validity_filters_denominator(df_base):
    out = apply_validity_filters(df_base, denominator_limit=5, den_col="den")
    assert (out["den"] >= 5).all()
    assert len(out) == 3  # den is [5,5,0,100] -> remove expert_id 3


def test_apply_validity_filters_missing_den_col(df_base):
    with pytest.raises(KeyError):
        apply_validity_filters(df_base.drop(columns=["den"]), denominator_limit=2, den_col="den")


def test_apply_eligibility_site_exclude_contains_case_insensitive(df_base):
    out = apply_eligibility_filters(df_base, site_exclude_contains=["trn"], filters=[], site_col="site")
    assert "TRN_site" not in out["site"].tolist()
    assert "c_trn" not in out["site"].tolist()
    assert len(out) == 2


def test_apply_eligibility_generic_filters(df_base):
    out = apply_eligibility_filters(
        df_base,
        site_exclude_contains=[],
        filters=[{"column": "talk_time_seconds", "op": ">=", "value": 2000}],
        site_col="site",
    )
    assert (out["talk_time_seconds"] >= 2000).all()
    assert set(out["expert_id"]) == {1, 2, 4}


def test_apply_eligibility_blank_sections_no_change(df_base):
    out = apply_eligibility_filters(df_base, site_exclude_contains=None, filters=None, site_col="site")
    assert out.equals(df_base)
