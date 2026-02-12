"""I/O helpers: safe filenames and versioned file export."""

from pathlib import Path
from datetime import datetime
import re
from typing import Optional


FILENAME_SAFE_RE = re.compile(r"[^A-Za-z0-9._-]")


def safe_filename(name: str) -> str:
    """
    Convert an arbitrary string into a safe filename fragment:
    - replace unsafe chars with underscore
    - collapse multiple underscores
    """
    sanitized = FILENAME_SAFE_RE.sub("_", name)
    sanitized = re.sub(r"__+", "_", sanitized)
    return sanitized.strip("_")


def versioned_excel_path(output_dir: str, base_name: str, ext: str = ".xlsx") -> str:
    """
    Return a non-colliding path in output_dir for base_name with today's date appended.
    If a file exists with the same name, append _v2, _v3, etc.
    """
    base_dir = Path(output_dir)
    today = datetime.today().strftime("%Y-%m-%d")
    base_dir.mkdir(parents=True, exist_ok=True)

    safe_base = safe_filename(base_name)
    candidate = base_dir / f"{safe_base}_{today}{ext}"
    if not candidate.exists():
        return str(candidate)

    # find next free version
    v = 2
    while True:
        candidate_v = base_dir / f"{safe_base}_{today}_v{v}{ext}"
        if not candidate_v.exists():
            return str(candidate_v)
        v += 1
