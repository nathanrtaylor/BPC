import pandas as pd
from pathlib import Path

import src.runner as runner


def test_runner_filters_to_comparison_outliers_and_promotes(monkeypatch, tmp_path, df_base, job_with_comparison_and_output):
    """
    Ensures:
    - comparison flag column exists
    - output shaping filters to only True flags
    - num/den/calc are promoted from comparison
    """
     # ✅ create dummy sql template files so runner's existence check passes
    (tmp_path / "sql_templates").mkdir(parents=True, exist_ok=True)
    (tmp_path / "sql_templates" / "epm_metric_data_generic.sql").write_text("select 1", encoding="utf-8")
    (tmp_path / "sql_templates" / "epm_metric_data_generic_filtered.sql").write_text("select 1", encoding="utf-8")

    # Make eligibility NOT remove comparison targets; set sites non-trn for all
    df_primary = df_base.copy()
    df_primary["site"] = ["a", "b", "c", "d"]  # no trn exclusions

    # Primary job query stubs
    monkeypatch.setattr(runner, "compile_sql", lambda engine, sql_path, params: "SELECT 1")
    monkeypatch.setattr(pd, "read_sql_query", lambda sql, engine: df_primary)

    # Comparison query returns cancellation metric; design so expert 2 and 4 are outliers for lower_is_good worst with limit=0.5
    # worst for lower_is_good should mean calc >= 0.5
    df_comp = pd.DataFrame({
        "expert_id": [1, 2, 3, 4],
        "num": [1, 1, 1, 1],
        "den": [1, 1, 1, 1],
        "calc": [0.2, 0.8, 0.1, 0.6],
    })
    monkeypatch.setattr(runner, "run_sql", lambda engine, sql_text, params, expanding_keys=None: df_comp)

    # Params builder stub
    monkeypatch.setattr(runner, "build_query_params", lambda job: job.get("inputs", {}))

    # IO stubs to avoid writing real excel
    monkeypatch.setattr(runner, "versioned_excel_path", lambda out_dir, base_name: str(tmp_path / f"{base_name}.xlsx"))
    monkeypatch.setattr(pd.DataFrame, "to_excel", lambda self, path, index=False: None)

    # Execute
    defaults = {"sql_templates_dir": "sql_templates", "output_dir": str(tmp_path), "log_dir": str(tmp_path)}
    engine = object()              # <-- define engine
    project_root = tmp_path

    meta = runner.run_job(engine, job_with_comparison_and_output, defaults, project_root)

    df_out = meta["df"]
    assert "cancellation_rate__is_outlier" in df_out.columns

    # Must be filtered to only True
    assert df_out["cancellation_rate__is_outlier"].all()
    assert set(df_out["expert_id"]) == {2, 4}

    # Promoted columns should match comparison values
    r = df_out.set_index("expert_id")
    assert r.loc[2, "calc"] == 0.8
    assert r.loc[4, "calc"] == 0.6
