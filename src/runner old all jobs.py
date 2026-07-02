# src/runner.py
"""
Main orchestration: load config, run jobs, save outputs.

Pipeline stages (explicit):
1) Query (raw data pull)
2) Validity filters      (statistical reliability guards; e.g., denominator_limit)
3) Eligibility filters   (business/operational gates; e.g., exclude "trn" in site, talk time thresholds)
4) Selection             (outlier selection: percentile/calc + best/worst)
5) Comparisons           (optional secondary metric pulls for cohort expert_ids + join + optional flagging)
6) Output shaping        (optional: filter to comparison outliers; promote comparison num/den/calc)
7) Enrichments           (optional post-shaping lookups for final expert_ids + join; e.g., call recording links)

Path behavior:
- PROJECT ROOT inferred as parent of config directory (expects <root>/configs/jobs.yml)
- sql_templates_dir / output_dir / log_dir resolved relative to PROJECT ROOT unless absolute paths

Config behavior:
- Sections may be omitted or blank; safe defaults apply.
- defaults.inputs (optional) can define shared input params like date_start/date_end for all jobs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, List, Optional
import logging

import pandas as pd
import yaml
from sqlalchemy import text, bindparam

from .logging_config import configure_logging
from .engine import build_presto_engine
from .query_params import build_query_params
from .sql_compiler import compile_sql
from .outliers import compute_outliers
from .io_utils import versioned_excel_path, safe_filename
from .utils import apply_validity_filters, apply_eligibility_filters

logger = logging.getLogger(__name__)


# ---------------------------
# Config / Path helpers
# ---------------------------

def load_config(path: str) -> Dict[str, Any]:
    """Load YAML config (Windows-friendly)."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with p.open("r", encoding="utf-8-sig") as fh:
        cfg = yaml.safe_load(fh)
    if not isinstance(cfg, dict):
        raise ValueError("Config file did not parse into a dictionary. Check YAML structure.")
    return cfg


def infer_project_root(config_path: Path) -> Path:
    """Assumes config lives at <project_root>/configs/*.yml."""
    cfg_path = config_path.resolve()
    return cfg_path.parent.parent


def resolve_relative_to_project_root(project_root: Path, configured_path: str) -> Path:
    """Resolve a path relative to project_root unless already absolute."""
    p = Path(configured_path)
    return p if p.is_absolute() else (project_root / p).resolve()


def resolve_template_path(project_root: Path, sql_templates_dir: str, template_name: str) -> Path:
    base_dir = resolve_relative_to_project_root(project_root, sql_templates_dir)
    return (base_dir / template_name).resolve()


def read_text_utf8_sig(path: Path) -> str:
    """Read text (SQL template) using utf-8-sig to avoid Windows BOM issues."""
    return path.read_text(encoding="utf-8-sig")


def apply_default_inputs(job: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge defaults.inputs into job.inputs (job overrides defaults).
    This lets you set date_start/date_end once in the YAML header.
    """
    default_inputs = (defaults.get("inputs") or {})
    job_inputs = (job.get("inputs") or {})
    merged = {**default_inputs, **job_inputs}
    job2 = dict(job)
    job2["inputs"] = merged
    return job2


# ---------------------------
# SQL execution helper (supports expanding IN lists)
# ---------------------------

def run_sql(engine, sql_text: str, params: Dict[str, Any], expanding_keys: Optional[List[str]] = None) -> pd.DataFrame:
    stmt = text(sql_text)
    if expanding_keys:
        for k in expanding_keys:
            stmt = stmt.bindparams(bindparam(k, expanding=True))
    return pd.read_sql(stmt, engine, params=params)


# ---------------------------
# Job execution
# ---------------------------

def run_job(engine, job: Dict[str, Any], defaults: Dict[str, Any], project_root: Path) -> Dict[str, Any]:
    job = apply_default_inputs(job, defaults)

    job_name = job.get("name") or "unnamed"
    logger.info("Starting job: %s", job_name)

    sql_templates_dir = defaults.get("sql_templates_dir", "sql_templates")

    # ----- Stage 1: Query -----
    params = build_query_params(job)

    sql_template_name = job.get("query", {}).get("sql_template")
    if not sql_template_name:
        raise KeyError("Job['query']['sql_template'] is required")

    sql_path = resolve_template_path(project_root, sql_templates_dir, sql_template_name)
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL template not found: {sql_path}")

    # Primary jobs can use literalized SQL via compile_sql
    sql = compile_sql(engine, str(sql_path), params)
    df_raw = pd.read_sql_query(sql, engine)
    logger.info("[%s] Raw query rows: %d", job_name, len(df_raw))

    # ----- Stage 2: Validity filters -----
    validity = job.get("validity", {}) or {}
    selection = job.get("selection", {}) or {}
    den_col = selection.get("den_col", "den")

    df_valid = apply_validity_filters(
        df_raw,
        denominator_limit=validity.get("denominator_limit"),
        den_col=den_col,
    )
    logger.info("[%s] After validity filters rows: %d", job_name, len(df_valid))

    # ----- Stage 3: Eligibility filters -----
    elig = job.get("eligibility", {}) or {}
    df_eligible = apply_eligibility_filters(
        df_valid,
        site_exclude_contains=elig.get("site_exclude_contains"),
        filters=elig.get("filters"),
        site_col=elig.get("site_col", "site"),
    )
    logger.info("[%s] After eligibility filters rows: %d", job_name, len(df_eligible))

    # ----- Stage 4: Selection (outliers) -----
    calc_col = selection.get("calc_col", "calc")

    df_out = compute_outliers(
        df_eligible,
        outlier_limit_target=selection.get("outlier_limit_target", "percentile"),
        outlier_limit=selection.get("outlier_limit", 0.95),
        metric_direction=selection.get("metric_direction", "higher_is_good"),
        outlier_side=selection.get("outlier_side", "worst"),
        # validity already filtered den; keep >=1 to avoid double-filter surprises
        denominator_limit=1,
        calc_col=calc_col,
        den_col=den_col,
    )
    logger.info("[%s] Outlier cohort rows: %d", job_name, len(df_out))

    # Add downstream metadata (optional)
    df_out = df_out.copy()
    df_out["cohort"] = job.get("inputs", {}).get("cohort_id", "")
    df_out["coaching_override"] = job.get("coaching_override", "")
    # Avoid colliding with enrichment recording_link column
    df_out["recording_link"] = job.get("reference_link", "")

    # ----- Stage 5: Comparisons (optional) -----
    comparisons = job.get("comparisons") or []
    if comparisons and not df_out.empty:
        if "expert_id" not in df_out.columns:
            raise KeyError("Comparisons require 'expert_id' column in the outlier cohort output.")

        cohort_ids = df_out["expert_id"].dropna().unique().tolist()
        logger.info("[%s] Running %d comparison(s) for %d expert_id(s)", job_name, len(comparisons), len(cohort_ids))

        for comp in comparisons:
            comp_name = comp.get("name", "comparison")
            comp_query = comp.get("query", {}) or {}
            comp_template = comp_query.get("sql_template")
            if not comp_template:
                raise KeyError(f"Comparison '{comp_name}' missing query.sql_template")

            # Build comparison job definition by overriding inputs (commonly metric)
            comp_inputs_override = comp.get("inputs_override", {}) or {}
            comp_job = {
                "query": comp_query,
                "inputs": {**(job.get("inputs", {}) or {}), **comp_inputs_override},
            }
            comp_job = apply_default_inputs(comp_job, defaults)

            comp_params = build_query_params(comp_job)
            comp_params["expert_ids"] = cohort_ids

            comp_sql_path = resolve_template_path(project_root, sql_templates_dir, comp_template)
            if not comp_sql_path.exists():
                raise FileNotFoundError(f"Comparison SQL template not found: {comp_sql_path}")

            # Comparisons must use expanding IN params (NOT compile_sql)
            comp_sql_text = read_text_utf8_sig(comp_sql_path)
            comp_df = run_sql(engine, comp_sql_text, comp_params, expanding_keys=["expert_ids"])
            logger.info("[%s] Comparison '%s' rows: %d", job_name, comp_name, len(comp_df))

            # Prefix comparison columns (except join key)
            prefix = f"{comp_name}__"
            comp_df = comp_df.rename(columns={c: f"{prefix}{c}" for c in comp_df.columns if c != "expert_id"})

            # Join back to cohort
            join_cfg = comp.get("join", {}) or {}
            join_on = join_cfg.get("on", ["expert_id"])
            how = join_cfg.get("how", "left")
            df_out = df_out.merge(comp_df, on=join_on, how=how)

            # Optional: compute a comparison outlier flag based on comparison selection config
            flag_sel = comp.get("selection") or comp.get("flag")  # allow either key
            if flag_sel:
                comp_calc_col = flag_sel.get("calc_col", f"{prefix}calc")
                comp_den_col = flag_sel.get("den_col", f"{prefix}den")

                tmp = df_out[["expert_id", comp_calc_col, comp_den_col]].dropna().copy()

                if not tmp.empty:
                    flagged = compute_outliers(
                        tmp,
                        outlier_limit_target=flag_sel.get("outlier_limit_target", "percentile"),
                        outlier_limit=flag_sel.get("outlier_limit", 0.95),
                        metric_direction=flag_sel.get("metric_direction", "higher_is_good"),
                        outlier_side=flag_sel.get("outlier_side", "worst"),
                        denominator_limit=flag_sel.get("denominator_limit", 1),
                        calc_col=comp_calc_col,
                        den_col=comp_den_col,
                    )
                    df_out[f"{prefix}is_outlier"] = df_out["expert_id"].isin(flagged["expert_id"])
                else:
                    df_out[f"{prefix}is_outlier"] = False

    elif comparisons and df_out.empty:
        logger.info("[%s] Cohort empty; skipping comparisons.", job_name)

    # ----- Stage 6: Output shaping (optional) -----
    # Supports:
    #  - filter_to_comparison_outliers: <comparison_name>
    #  - promote_comparison_metrics: <comparison_name>
    #  - keep_original_primary_metrics: true/false
    output_cfg = job.get("output", {}) or {}

    # 6a) Filter to only comparison outliers
    comp_filter_name = output_cfg.get("filter_to_comparison_outliers")
    if comp_filter_name:
        flag_col = f"{comp_filter_name}__is_outlier"
        if flag_col not in df_out.columns:
            raise KeyError(
                f"Output requested filter_to_comparison_outliers='{comp_filter_name}' "
                f"but missing column '{flag_col}'. Ensure the comparison has a 'selection' block."
            )
        df_out = df_out[df_out[flag_col] == True].copy()
        logger.info("[%s] After output filter (%s) rows: %d", job_name, flag_col, len(df_out))

    # 6b) Promote comparison num/den/calc to primary num/den/calc
    promote_name = output_cfg.get("promote_comparison_metrics")
    if promote_name:
        comp_num = f"{promote_name}__num"
        comp_den = f"{promote_name}__den"
        comp_calc = f"{promote_name}__calc"

        for c in (comp_den, comp_calc):
            if c not in df_out.columns:
                raise KeyError(
                    f"Output requested promote_comparison_metrics='{promote_name}' "
                    f"but missing column '{c}'. Ensure the comparison query returns num/den/calc."
                )

        keep_orig = bool(output_cfg.get("keep_original_primary_metrics", True))

        # Preserve current primary metrics if requested
        if keep_orig:
            if "num" in df_out.columns and "primary__num" not in df_out.columns:
                df_out = df_out.rename(columns={"num": "primary__num"})
            if "den" in df_out.columns and "primary__den" not in df_out.columns:
                df_out = df_out.rename(columns={"den": "primary__den"})
            if "calc" in df_out.columns and "primary__calc" not in df_out.columns:
                df_out = df_out.rename(columns={"calc": "primary__calc"})
        else:
            for c in ("num", "den", "calc"):
                if c in df_out.columns:
                    df_out = df_out.drop(columns=[c])

        # Promote comparison metrics to primary output columns
        if comp_num in df_out.columns:
            df_out["num"] = df_out[comp_num]
        df_out["den"] = df_out[comp_den]
        df_out["calc"] = df_out[comp_calc]

        logger.info("[%s] Promoted %s__num/den/calc to primary num/den/calc", job_name, promote_name)

    # ----- Stage 7: Enrichments (optional; post-shaping) -----
    # This runs AFTER output shaping so it only queries for the final expert set written to Excel.
    enrichments = job.get("enrichments") or []
    if enrichments and not df_out.empty:
        if "expert_id" not in df_out.columns:
            raise KeyError("Enrichments require 'expert_id' column in the output dataframe.")

        final_ids = df_out["expert_id"].dropna().unique().tolist()
        logger.info("[%s] Running %d enrichment(s) for %d expert_id(s)", job_name, len(enrichments), len(final_ids))

        for enr in enrichments:
            enr_name = enr.get("name", "enrichment")
            enr_query = enr.get("query", {}) or {}
            enr_template = enr_query.get("sql_template")
            if not enr_template:
                raise KeyError(f"Enrichment '{enr_name}' missing query.sql_template")

            enr_inputs_override = enr.get("inputs_override", {}) or {}
            enr_job = {
                "query": enr_query,
                "inputs": {**(job.get("inputs", {}) or {}), **enr_inputs_override},
            }
            enr_job = apply_default_inputs(enr_job, defaults)

            enr_params = build_query_params(enr_job)
            enr_params["expert_ids"] = final_ids

            enr_sql_path = resolve_template_path(project_root, sql_templates_dir, enr_template)
            if not enr_sql_path.exists():
                raise FileNotFoundError(f"Enrichment SQL template not found: {enr_sql_path}")

            enr_sql_text = read_text_utf8_sig(enr_sql_path)
            enr_df = run_sql(engine, enr_sql_text, enr_params, expanding_keys=["expert_ids"])
            logger.info("[%s] Enrichment '%s' rows: %d", job_name, enr_name, len(enr_df))

            # Optional: map/rename enrichment columns (e.g., {"recording_link":"recording_link"})
            col_map = enr.get("map_columns", {}) or {}
            if col_map:
                enr_df = enr_df.rename(columns=col_map)

            join_cfg = enr.get("join", {}) or {}
            join_on = join_cfg.get("on", ["expert_id"])
            how = join_cfg.get("how", "left")

            df_out = df_out.merge(
                enr_df,
                on=join_on,
                how=how,
                suffixes=("", "__enr")
            )

            # If enrichment provided reference_link, overwrite original
            if "recording_link__enr" in df_out.columns:
                df_out["recording_link"] = df_out["recording_link__enr"]
                df_out = df_out.drop(columns=["recording_link__enr"])

    elif enrichments and df_out.empty:
        logger.info("[%s] Output empty; skipping enrichments.", job_name)

    # ----- Output -----
    output_dir_cfg = defaults.get("output_dir", "outputs/excel")
    output_dir = resolve_relative_to_project_root(project_root, output_dir_cfg)
    output_dir.mkdir(parents=True, exist_ok=True)

    safe_job = safe_filename(job_name)
    cohort_id = job.get("inputs", {}).get("cohort_id", "")
    base_name = f"{safe_job}_cohort_{cohort_id}"

    out_path = versioned_excel_path(str(output_dir), base_name)
    df_out.to_excel(out_path, index=False)
    logger.info("[%s] Wrote output to %s (rows=%d)", job_name, out_path, len(df_out))

    return {
        "job_name": job_name,
        "status": "success",
        "rows_raw": int(len(df_raw)),
        "rows_valid": int(len(df_valid)),
        "rows_eligible": int(len(df_eligible)),
        "rows_out": int(len(df_out)),
        "out_path": out_path,
        "df": df_out,
    }


def run_all_jobs(config_path: str, dry_run: bool = False) -> Dict[str, Any]:
    cfg_path = Path(config_path).resolve()
    project_root = infer_project_root(cfg_path)

    cfg = load_config(str(cfg_path))
    defaults = cfg.get("defaults", {}) or {}

    # Logging
    log_dir_cfg = defaults.get("log_dir", "logs")
    log_dir = resolve_relative_to_project_root(project_root, log_dir_cfg)
    configure_logging(str(log_dir))

    engine = build_presto_engine()

    results: Dict[str, Any] = {}
    jobs = cfg.get("jobs", []) or []

    for job in jobs:
        job = apply_default_inputs(job, defaults)
        job_name = job.get("name", "unnamed")
        try:
            if dry_run:
                logger.info("Dry run: validating %s", job_name)
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
    parser.add_argument("config", help="Path to YAML config file (e.g. configs/jobs_new_format.yml)")
    parser.add_argument("--dry-run", action="store_true", help="Validate jobs without executing queries or saving outputs")
    args = parser.parse_args()

    res = run_all_jobs(args.config, dry_run=args.dry_run)
    for k, v in res.items():
        print(f"{k}: {v.get('status')} (rows_out={v.get('rows_out', '-')})")