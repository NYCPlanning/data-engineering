"""Ingest lifecycle module."""

from pathlib import Path
from typing import List

from dcpy.configuration import INGEST_DEF_DIR
from dcpy.lifecycle.asset_models import IngestTemplate


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


def list_ingest_templates() -> List[IngestTemplate]:
    """List all available ingest templates.

    Returns:
        List of IngestTemplate objects with name and path attributes

    Raises:
        FileNotFoundError: If the templates directory doesn't exist
    """
    template_dir = get_template_directory()

    templates = []
    for template_file in sorted(template_dir.glob("*.yml")):
        templates.append(
            IngestTemplate(
                name=template_file.stem,
                path=template_file,
            )
        )

    return templates


__all__ = ["get_template_directory", "list_ingest_templates"]
