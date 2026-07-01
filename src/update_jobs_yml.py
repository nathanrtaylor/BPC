#!/usr/bin/env python3
"""
Duplicate and roll forward a jobs YAML file.

Example:
    python update_jobs_yml.py configs/jobs_062526.yml

This creates a copy beside the input file named jobs_MMDDYY.yml, where MMDDYY
is today's date, then updates:
  - date_end:   'YYYY-MM-DD' to the upcoming Friday
  - date_start: 'YYYY-MM-DD' to 14 days before that Friday
  - cohort_id:  'N'          to 'N+1', scanning from 150 down to 1

The script uses text replacement so comments, ordering, and formatting are
preserved as much as possible.
"""

from __future__ import annotations

import argparse
import re
import shutil
from datetime import date, timedelta
from pathlib import Path


DATE_END_RE = re.compile(r"(date_end:\s*)'\d{4}-\d{2}-\d{2}'")
DATE_START_RE = re.compile(r"(date_start:\s*)'\d{4}-\d{2}-\d{2}'")


def upcoming_friday(today: date | None = None) -> date:
    """Return the next Friday on or after today."""
    today = today or date.today()
    friday_weekday = 4  # Monday=0, Friday=4
    days_until_friday = (friday_weekday - today.weekday()) % 7
    return today + timedelta(days=days_until_friday)


def output_path_for(input_path: Path, today: date | None = None) -> Path:
    """Build jobs_MMDDYY.yml beside the input file."""
    today = today or date.today()
    return input_path.with_name(f"jobs_{today:%m%d%y}{input_path.suffix or '.yml'}")


def increment_cohort_ids(text: str, start_value: int = 150) -> str:
    """
    Increment cohort_id values by 1.

    The loop intentionally scans downward from start_value to 1 so that a value
    changed from 149 to 150 will not be changed again when 150 is processed.
    """
    for value in range(start_value, 0, -1):
        pattern = re.compile(rf"(cohort_id:\s*)'{value}'")
        text = pattern.sub(rf"\g<1>'{value + 1}'", text)
    return text


def update_jobs_yaml(input_file: str | Path, cohort_start: int = 150) -> Path:
    """Copy the input YAML to jobs_MMDDYY.yml and apply date/cohort updates."""
    input_path = Path(input_file)
    if not input_path.is_file():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    output_path = output_path_for(input_path)
    shutil.copy2(input_path, output_path)

    text = output_path.read_text(encoding="utf-8")

    end_date = upcoming_friday()
    # This follows the example: 2026-07-03 -> 2026-06-19.
    start_date = end_date - timedelta(days=14)

    text = DATE_END_RE.sub(rf"\g<1>'{end_date:%Y-%m-%d}'", text)
    text = DATE_START_RE.sub(rf"\g<1>'{start_date:%Y-%m-%d}'", text)
    text = increment_cohort_ids(text, start_value=cohort_start)

    output_path.write_text(text, encoding="utf-8")
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Duplicate a jobs YAML file and roll dates/cohort IDs forward."
    )
    parser.add_argument(
        "input_file",
        help='Path to the input YAML file, for example "configs/jobs_062526.yml".',
    )
    parser.add_argument(
        "--cohort-start",
        type=int,
        default=150,
        help="Starting cohort_id value for the decrementing update loop. Default: 150.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = update_jobs_yaml(args.input_file, cohort_start=args.cohort_start)
    print(f"Created updated file: {output_path}")


if __name__ == "__main__":
    main()
