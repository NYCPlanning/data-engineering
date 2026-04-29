"""Ingest lifecycle module."""
from pathlib import Path

from dcpy.configuration import INGEST_DEF_DIR


def get_template_directory() -> Path:
    """Get the ingest templates directory and assert it exists.

    Returns:
        Path to the ingest templates directory

    Raises:
        FileNotFoundError: If the templates directory doesn't exist
    """
    template_dir = INGEST_DEF_DIR

    if not template_dir.exists():
        raise FileNotFoundError(
            f"Templates directory not found at {template_dir}. "
            f"Set TEMPLATE_DIR environment variable to the correct path."
        )

    return template_dir


__all__ = ["get_template_directory"]
