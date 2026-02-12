import pandas as pd
import pytest


@pytest.fixture
def df_base():
    # Minimal dataset with everything runner expects
    return pd.DataFrame({
        "expert_id": [1, 2, 3, 4],
        "site": ["A", "TRN_site", "b", "c_trn"],
        "num": [10, 20, 30, 40],
        "den": [5, 5, 0, 100],
        "calc": [0.1, 0.2, 0.3, 0.9],
        "talk_time_seconds": [2500, 3000, 100, 2500],
    })


@pytest.fixture
def job_with_comparison_and_output():
    # Designed so comparison flag should keep only expert_id 2 and 4 in final output
    return {
        "name": "JOB_A",
        "query": {"query_type": "metric", "sql_template": "epm_metric_data_generic.sql"},
        "inputs": {
            "client": "X",
            "metric": "ERP",
            "cohort_id": "13",
            "date_start": "2026-01-30",
            "date_end": "2026-02-13",
        },
        "validity": {"denominator_limit": 1},
        "eligibility": {
            "site_exclude_contains": ["trn"],  # will exclude rows 2 and 4 if applied; we will set sites accordingly in test
            "filters": [],
        },
        "selection": {
            "outlier_limit_target": "percentile",
            "outlier_limit": 0.0,   # keep all for simplicity in tests
            "metric_direction": "higher_is_good",
            "outlier_side": "best",
        },
        "comparisons": [
            {
                "name": "cancellation_rate",
                "query": {"query_type": "metric", "sql_template": "epm_metric_data_generic_filtered.sql"},
                "inputs_override": {"metric": "Cancellation Rate"},
                "join": {"on": ["expert_id"], "how": "left"},
                "selection": {
                    "outlier_limit_target": "calc",
                    "outlier_limit": 0.5,
                    "metric_direction": "lower_is_good",
                    "outlier_side": "worst",
                },
            }
        ],
        "output": {
            "filter_to_comparison_outliers": "cancellation_rate",
            "promote_comparison_metrics": "cancellation_rate",
            "keep_original_primary_metrics": False,
        },
    }
