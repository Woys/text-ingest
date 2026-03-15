"""Minimal structured logging helper."""

from __future__ import annotations

import logging
import os


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured from the MDI_LOG_LEVEL environment variable."""
    level_name = os.getenv("MDI_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        )

    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger
