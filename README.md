# Outlier Pipeline (Config-Driven Jupyter Process)

This repository replaces "one notebook per metric/client" with a
config-driven pipeline that runs many metric analyses in a loop and
writes consistent Excel outputs each week.

------------------------------------------------------------------------

## High-level Architecture

Each job runs through five explicit stages:

1.  **Query**\
    Pulls raw data using a SQL template and parameters built from YAML
    inputs.

2.  **Validity**\
    Applies statistical reliability guards (e.g., denominator
    thresholds).

3.  **Eligibility**\
    Applies business/operational filters (e.g., exclude sites containing
    "trn", talk time thresholds).

4.  **Selection**\
    Performs outlier selection using percentile or calc thresholds,
    metric direction, and best/worst tail logic.

5.  **Comparisons (Optional)**\
    Runs secondary metric queries for the selected cohort and joins
    results back for enrichment or flagging.

------------------------------------------------------------------------

## Directory Structure

    outlier_pipeline/
    ├─ configs/
    │  └─ jobs_new_format.yml
    ├─ sql_templates/
    │  ├─ epm_metric_data_generic.sql
    │  └─ epm_metric_data_generic_filtered.sql
    ├─ src/
    │  ├─ runner.py
    │  ├─ utils.py
    │  ├─ outliers.py
    │  ├─ query_params.py
    │  ├─ sql_compiler.py
    │  ├─ engine.py
    │  ├─ io_utils.py
    │  ├─ logging_config.py
    │  └─ __init__.py
    └─ notebooks/
       └─ run_all_jobs.ipynb

------------------------------------------------------------------------

## Key Files to Maintain

### configs/jobs_new_format.yml

Primary configuration file.\
Edit this file to: - Add new jobs - Adjust thresholds - Update date
ranges - Add comparisons

### sql_templates/

Contains SQL used by jobs. - Base templates for full population
queries - `_filtered` versions that include:
`sql   AND expert_id IN :expert_ids`

### src/runner.py

Controls the full pipeline execution and stage ordering.

### src/utils.py

Contains validity and eligibility filtering logic.

### src/outliers.py

Contains outlier selection logic (percentile/calc, direction,
best/worst).

### src/query_params.py

Maps YAML inputs to SQL parameters.

------------------------------------------------------------------------

## Adding a New Job

1.  Duplicate an existing job block in `jobs_new_format.yml`.
2.  Update:
    -   `name`
    -   `inputs.client`
    -   `inputs.metric`
    -   `selection` parameters
3.  Ensure the correct SQL template exists.
4.  Run `run_all_jobs()`.

------------------------------------------------------------------------

## Example Job Structure

``` yaml
- name: SAMPLE_JOB

  query:
    query_type: metric
    sql_template: epm_metric_data_generic.sql

  inputs:
    client: "MOB-AT&T"
    metric: "CRT"
    cohort_id: "1"
    date_start: "2026-01-30"
    date_end: "2026-02-13"

  validity:
    denominator_limit: 10

  eligibility:
    site_exclude_contains:
      - trn
    filters: []

  selection:
    outlier_limit_target: percentile
    outlier_limit: 0.90
    metric_direction: higher_is_good
    outlier_side: worst

  comparisons: []
```

------------------------------------------------------------------------

## Running the Pipeline

In Jupyter:

``` python
from pathlib import Path
from src.runner import run_all_jobs

CONFIG_PATH = Path("configs/jobs_new_format.yml")
results = run_all_jobs(str(CONFIG_PATH))
```

------------------------------------------------------------------------

## Weekly Maintenance Checklist

-   Update date ranges (or defaults section)
-   Add/modify job blocks as needed
-   Confirm SQL templates align with expected columns
-   Run pipeline and review summary output

------------------------------------------------------------------------

## Notes

-   `site_exclude_contains: ["trn"]` removes any row where the site
    column contains "trn" (case-insensitive).
-   Comparisons require SQL templates that support
    `expert_id IN :expert_ids`.
-   Always restart the Jupyter kernel after modifying source files.
