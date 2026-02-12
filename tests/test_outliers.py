import pandas as pd
from src.outliers import compute_outliers


def test_calc_target_lower_is_good_worst_selects_high_values():
    df = pd.DataFrame({"expert_id": [1, 2, 3], "calc": [0.1, 0.6, 0.4], "den": [10, 10, 10]})
    out = compute_outliers(
        df,
        outlier_limit_target="calc",
        outlier_limit=0.5,
        metric_direction="lower_is_good",
        outlier_side="worst",
        denominator_limit=1,
        calc_col="calc",
        den_col="den",
    )
    # worst for lower_is_good should include calc >= 0.5 -> expert 2
    assert out["expert_id"].tolist() == [2]


def test_calc_target_higher_is_good_worst_selects_low_values():
    df = pd.DataFrame({"expert_id": [1, 2, 3], "calc": [0.1, 0.6, 0.4], "den": [10, 10, 10]})
    out = compute_outliers(
        df,
        outlier_limit_target="calc",
        outlier_limit=0.25,
        metric_direction="higher_is_good",
        outlier_side="worst",
        denominator_limit=1,
        calc_col="calc",
        den_col="den",
    )
    # worst for higher_is_good should include calc < 0.25 (or <= depending on your implementation)
    assert out["expert_id"].tolist() == [1]


def test_percentile_target_best_higher_is_good_selects_top_tail():
    df = pd.DataFrame({"expert_id": [1, 2, 3, 4], "calc": [10, 20, 30, 40], "den": [1, 1, 1, 1]})
    out = compute_outliers(
        df,
        outlier_limit_target="percentile",
        outlier_limit=0.75,
        metric_direction="higher_is_good",
        outlier_side="best",
        denominator_limit=1,
    )
    assert set(out["expert_id"]) == {3, 4}  # top 25% (approx) depending on rank method
