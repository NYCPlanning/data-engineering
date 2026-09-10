"""Data models for lifecycle assets (products, templates, etc.)."""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class Product:
    """Represents a product in the products directory."""

    name: str
    path: Path

    @property
    def recipe_path(self) -> Path:
        """Path to the product's recipe.yml file."""
        return self.path / "recipe.yml"


@dataclass
class IngestTemplate:
    """Represents an ingest template."""

    name: str
    path: Path
