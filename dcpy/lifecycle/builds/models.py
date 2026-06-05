from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
from pydantic import BaseModel, field_validator, model_serializer

from dcpy.connectors.edm import models as recipes

if TYPE_CHECKING:
    from dcpy.lifecycle.builds.plan.models import InputDataset, InputDatasetDestination, Recipe

# TODO: Fix circular import caused by architectural issue
# ========================================================
# We use Any for Recipe type to avoid circular import at module load time.
# The circular import chain is:
#   dcpy/connectors/edm/publishing.py (imports BuildMetadata)
#     → dcpy/lifecycle/builds/models.py (imports Recipe)
#     → dcpy/lifecycle/builds/plan/models.py
#     → dcpy/lifecycle/builds/plan/recipe.py (imports publishing)
#     → back to publishing.py 🔄
#
# Root cause: publishing.py is in connectors/ but imports lifecycle models (backwards dependency).
# Proper architecture should be: lifecycle → connectors (never connectors → lifecycle)
#
# Potential solutions:
#   1. Move publishing.py to lifecycle/builds/artifacts/ (it's really lifecycle code)
#   2. Have publishing.py return plain dicts, let lifecycle handle deserialization
#   3. Extract BuildMetadata to a shared models module both can import
#
# Current workaround: Use Any + @field_validator for runtime type conversion
# Note: InputDataset/InputDatasetDestination have been moved to plan.models
# ========================================================


class ImportedDataset(BaseModel, extra="forbid", arbitrary_types_allowed=True):
    id: str
    version: str
    file_type: recipes.DatasetType
    destination: str | pd.DataFrame | Path
    destination_type: Any = None  # InputDatasetDestination

    @staticmethod
    def from_input(ds: Any, result: str | pd.DataFrame | Path) -> ImportedDataset:
        assert ds.version, f"Version of {ds.id} not resolved"
        assert ds.file_type, f"File type of {ds.id} not resolved"
        return ImportedDataset(
            id=ds.id,
            version=ds.version,
            file_type=ds.file_type,
            destination=result,
            destination_type=ds.destination,
        )

    @model_serializer
    def _model_dump(self):
        return {
            "id": self.id,
            "version": self.version,
            "file_type": self.file_type,
            "destination_type": self.destination_type,
            "destination": self.destination
            if type(self.destination) is not pd.DataFrame
            else "dataframe",
        }


class LoadResult(BaseModel, extra="forbid"):
    name: str
    build_name: str
    datasets: dict[str, dict[str, ImportedDataset]]

    def get_dataset_versions(self, ds_name: str) -> list[str]:
        return list(self.datasets[ds_name].keys())

    def get_latest_version_str(self, ds_name: str) -> str:
        return self.get_dataset_versions(ds_name)[-1]

    def get_latest_version(self, ds_name: str) -> ImportedDataset:
        return self.datasets[ds_name][self.get_latest_version_str(ds_name)]


class BuildMetadata(BaseModel, extra="forbid"):
    timestamp: datetime
    commit: str | None = None
    run_url: str | None = None
    version: str
    draft_revision_name: str | None = None
    recipe: Any  # Recipe - using Any to avoid circular import, validated below
    load_result: LoadResult | None = None

    @field_validator("recipe", mode="before")
    @classmethod
    def validate_recipe(cls, v):
        """Ensure recipe is a Recipe instance, not a dict."""
        if isinstance(v, dict):
            from dcpy.lifecycle.builds.plan.models import Recipe

            return Recipe(**v)
        return v

    def __init__(self, **data):
        if "version" not in data:
            recipe = data["recipe"]
            # Handle both Recipe objects and dicts
            version = (
                recipe.version if hasattr(recipe, "version") else recipe.get("version")
            )
            if version is not None:
                data["version"] = version
        super().__init__(**data)
