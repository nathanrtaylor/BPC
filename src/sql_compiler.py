# src/sql_compiler.py
"""Load SQL template and compile it with literal bound params using SQLAlchemy."""

from pathlib import Path
from sqlalchemy.sql import text, bindparam
from sqlalchemy.engine import Engine
from typing import Dict


def _read_text_with_fallback(path: Path) -> str:
    """
    Try common encodings in order. If all fail, read bytes and decode with replacement
    so the pipeline runs (you can then clean the file if needed).
    """
    for enc in ("utf-8", "utf-8-sig", "cp1252"):
        try:
            return path.read_text(encoding=enc)
        except UnicodeDecodeError:
            continue

    # last resort: replace bad characters
    raw = path.read_bytes()
    return raw.decode("utf-8", errors="replace")


def compile_sql(engine: Engine, sql_template_path: str, params: Dict[str, object]) -> str:
    p = Path(sql_template_path)
    if not p.exists():
        raise FileNotFoundError(f"SQL template not found: {sql_template_path}")

    sql_text = _read_text_with_fallback(p)

    stmt = text(sql_text).bindparams(**params)
    compiled = str(stmt.compile(engine, compile_kwargs={"literal_binds": True}))
    return compiled
