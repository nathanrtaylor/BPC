"""Logging configuration for the outlier pipeline."""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


def configure_logging(log_dir: str = "logs", level: int = logging.INFO) -> None:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(level)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    ch.setFormatter(ch_formatter)

    # Rotating file handler
    fh = RotatingFileHandler(Path(log_dir) / "outlier_pipeline.log", maxBytes=10_000_000, backupCount=5)
    fh.setLevel(level)
    fh_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    fh.setFormatter(fh_formatter)

    # Clear existing handlers to avoid duplicate logs in some runtimes
    if logger.handlers:
        logger.handlers = []

    logger.addHandler(ch)
    logger.addHandler(fh)
