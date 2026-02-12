from pathlib import Path
import pandas as pd
import src.runner as runner

def test_runner_handles_blank_validity_eligibility_comparisons(monkeypatch, tmp_path):
    job = {
        "name": "JOB_BLANKS",
        "query": {"query_type": "metric", "sql_template": "x.sql"},
        "inputs": {"client": "X", "metric": "Y", "cohort_id": "1", "date_start": "2026-01-01", "date_end": "2026-01-07"},
        "validity": None,
        "eligibility": None,
        "selection": {"outlier_limit_target": "percentile", "outlier_limit": 0.0, "metric_direction": "higher_is_good", "outlier_side": "best"},
        "comparisons": None,
        "output": None,
    }

    # ✅ create dummy sql template file so runner's existence check passes
    (tmp_path / "sql_templates").mkdir(parents=True, exist_ok=True)
    (tmp_path / "sql_templates" / "x.sql").write_text("select 1", encoding="utf-8")

    df_primary = pd.DataFrame({
        "expert_id": [1, 2],
        "site": ["a", "b"],
        "num": [1, 1],
        "den": [1, 1],
        "calc": [0.1, 0.2],
    })

    monkeypatch.setattr(runner, "compile_sql", lambda engine, sql_path, params: "SELECT 1")
    monkeypatch.setattr(pd, "read_sql_query", lambda sql, engine: df_primary)
    monkeypatch.setattr(runner, "build_query_params", lambda job: job.get("inputs", {}))
    monkeypatch.setattr(runner, "versioned_excel_path", lambda out_dir, base_name: str(tmp_path / f"{base_name}.xlsx"))
    monkeypatch.setattr(pd.DataFrame, "to_excel", lambda self, path, index=False: None)

    defaults = {"sql_templates_dir": "sql_templates", "output_dir": str(tmp_path), "log_dir": str(tmp_path)}
    meta = runner.run_job(object(), job, defaults, tmp_path)

    assert meta["status"] == "success"
    assert meta["rows_out"] == 2
