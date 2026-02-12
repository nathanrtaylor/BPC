# src/runner.py
"""
Main orchestration: load config, run jobs, save outputs.

Latest behavior:
- Reads YAML config using Windows-safe UTF-8 BOM handling (utf-8-sig).
- Defines PROJECT ROOT as the parent directory of the config directory (expects config at: <root>/configs/jobs.yml).
- Resolves sql_templates_dir, output_dir, and log_dir relative to PROJECT ROOT (unless absolute paths are supplied).
- Runs each job: compile SQL template + params, execute query, filter, compute outliers, add metadata, save versioned Excel.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, Optional
import logging

import pandas as pd
import yaml

from .logging_config import configure_logging
from .engine import build_presto_engine
from .query_params import build_query_params
from .sql_compiler import compile_sql
from .outliers import compute_outliers
from .io_utils import versioned_excel_path, safe_filename
from .utils import apply_site_exclusions

logger = logging.getLogger(__name__)


def load_config(path: str) -> Dict[str, Any]:
    """Load YAML config from path (Windows-friendly)."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    # utf-8-sig handles UTF-8 files with BOM (common on Windows)
    with p.open("r", encoding="utf-8-sig") as fh:
        cfg = yaml.safe_load(fh)
    if not isinstance(cfg, dict):
        raise ValueError("Config file did not parse into a dictionary. Check YAML structure.")
    return cfg


def infer_project_root(config_path: Path) -> Path:
    """
    Infer project root from a config path.

    Expected layout:
      <project_root>/configs/jobs.yml

    Therefore:
      project_root = config_path.parent.parent
    """
    cfg_path = config_path.resolve()
    cfg_dir = cfg_path.parent
    project_root = cfg_dir.parent
    return project_root


def resolve_relative_to_project_root(project_root: Path, configured_path: str) -> Path:
    """
    Resolve configured_path to an absolute path.
    - If configured_path is absolute, return it as Path.
    - Otherwise, resolve it relative to project_root.
    """
    p = Path(configured_path)
    if p.is_absolute():
        return p
    return (project_root / p).resolve()


def resolve_template_path(project_root: Path, sql_templates_dir: str, template_name: str) -> Path:
    """
    Resolve a template name to a full absolute path.
    If sql_templates_dir is absolute, use it directly.
    Otherwise, interpret it relative to project_root.
    """
    base_dir = resolve_relative_to_project_root(project_root, sql_templates_dir)
    return (base_dir / template_name).resolve()


def run_job(engine, job: Dict[str, Any], defaults: Dict[str, Any], project_root: Path) -> Dict[str, Any]:
    """Run a single job and export results."""
    job_name = job.get("name") or f"job_{job.get('inputs', {}).get('client', 'anon')}"
    logger.info("Starting job: %s", job_name)

    # Build SQL params from job inputs/query_type registry
    params = build_query_params(job)

    # Resolve SQL template path relative to project root
    sql_templates_dir = defaults.get("sql_templates_dir", "sql_templates")
    sql_template_name = job.get("query", {}).get("sql_template")
    if not sql_template_name:
        raise KeyError("Job['query']['sql_template'] is required")

    sql_path = resolve_template_path(project_root, sql_templates_dir, sql_template_name)
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL template not found: {sql_path}")

    # Compile and execute SQL
    sql = compile_sql(engine, str(sql_path), params)
    df = pd.read_sql_query(sql, engine)
    logger.info("[%s] Query returned %d rows", job_name, len(df))

    # Apply optional filters
    filters = job.get("filters", {}) or {}
    df_filtered = apply_site_exclusions(df, filters.get("exclude_site_suffixes"))

    # Compute outliers
    out_cfg = job.get("outliers", {}) or {}
    df_out = compute_outliers(
        df_filtered,
        outlier_limit_target=out_cfg.get("outlier_limit_target", "percentile"),
        outlier_limit=out_cfg.get("outlier_limit", 0.95),
        metric_direction=out_cfg.get("metric_direction", "higher_is_good"),
        outlier_side=out_cfg.get("outlier_side", "worst"),   # NEW
        denominator_limit=out_cfg.get("denominator_limit", 1),
        calc_col=out_cfg.get("calc_col", "calc"),
        den_col=out_cfg.get("den_col", "den"),
    )

    # Add downstream metadata columns
    df_out = df_out.copy()
    df_out["cohort"] = job.get("inputs", {}).get("cohort_id", "")
    df_out["coaching_override"] = job.get("coaching_override", "")
    df_out["recording_link"] = job.get("reference_link", "")

    # Resolve output directory relative to project root
    output_dir_cfg = defaults.get("output_dir", "outputs/excel")
    output_dir = resolve_relative_to_project_root(project_root, output_dir_cfg)
    output_dir.mkdir(parents=True, exist_ok=True)

    safe_job = safe_filename(job_name)
    cohort_id = job.get("inputs", {}).get("cohort_id", "")
    base_name = f"{safe_job}_cohort_{cohort_id}"

    out_path = versioned_excel_path(str(output_dir), base_name)
    df_out.to_excel(out_path, index=False)
    logger.info("[%s] Wrote outliers to %s (rows=%d)", job_name, out_path, len(df_out))

    return {
        "job_name": job_name,
        "status": "success",
        "rows_out": int(len(df_out)),
        "out_path": out_path,
        "df": df_out,
    }


def run_all_jobs(config_path: str, dry_run: bool = False) -> Dict[str, Any]:
    """
    Run all jobs defined in the YAML config at config_path.

    Paths:
    - PROJECT ROOT is inferred as parent of the config directory.
    - sql_templates_dir/output_dir/log_dir are resolved relative to PROJECT ROOT unless absolute paths are provided.
    """
    cfg_path = Path(config_path).resolve()
    project_root = infer_project_root(cfg_path)

    cfg = load_config(str(cfg_path))
    defaults = cfg.get("defaults", {}) or {}

    # Configure logging (relative to project root)
    log_dir_cfg = defaults.get("log_dir", "logs")
    log_dir = resolve_relative_to_project_root(project_root, log_dir_cfg)
    configure_logging(str(log_dir))

    # Build engine once
    engine = build_presto_engine()

    jobs = cfg.get("jobs", []) or []
    results: Dict[str, Any] = {}

    for job in jobs:
        job_name = job.get("name", "unnamed")
        try:
            if dry_run:
                logger.info("Dry run: validating job %s", job_name)
                # Validate params + template existence + SQL compilation
                params = build_query_params(job)

                sql_templates_dir = defaults.get("sql_templates_dir", "sql_templates")
                sql_template_name = job.get("query", {}).get("sql_template")
                if not sql_template_name:
                    raise KeyError("Job['query']['sql_template'] is required")

                sql_path = resolve_template_path(project_root, sql_templates_dir, sql_template_name)
                if not sql_path.exists():
                    raise FileNotFoundError(f"SQL template not found: {sql_path}")

                _ = compile_sql(engine, str(sql_path), params)
                results[job_name] = {"job_name": job_name, "status": "dry_run_validated"}
                continue

            meta = run_job(engine, job, defaults, project_root)
            results[job_name] = meta

        except Exception:
            logger.exception("Job failed: %s", job_name)
            results[job_name] = {"job_name": job_name, "status": "failed"}

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run outlier analysis jobs from a YAML config")
    parser.add_argument("config", help="Path to YAML config file (e.g. configs/jobs.yml)")
    parser.add_argument("--dry-run", action="store_true", help="Validate jobs without executing queries or saving outputs")
    args = parser.parse_args()

    res = run_all_jobs(args.config, dry_run=args.dry_run)
    for k, v in res.items():
        print(f"{k}: {v.get('status')} (rows_out={v.get('rows_out', '-')})")
