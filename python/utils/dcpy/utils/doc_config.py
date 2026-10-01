"""A declarative plan for assembling a `Doc`: what appears, in what order, and (for
dbt models) arbitrary per-model directives (does this one get a DAT field-layout table
or a plain column table? extra prose/headings? images?) - all as data, not as
inference logic buried in a product's script.

`DocConfig.items` is a single ordered list mixing two kinds of thing:
- `ModelGroup` - a named group of dbt models.
- `BoilerplateInclude` - a pointer to hand-authored sections (see
  `dcpy.utils.doc.load_sections`) to splice in at this position.

Both live in the same list *specifically* so ordering is controlled in exactly one
place: an intro can sit before every model group, an appendix after all of them, and
neither needs a special case - they're just items at different positions in the same
list.

This module deliberately knows nothing about dbt, DAT formatting, or images: `custom`
is where a product's own script puts whatever per-model directives it needs, the same
pattern `dcpy.product_metadata`'s `CustomizableBase` already uses. A model belongs in a
`DocConfig` because a human declared it here, in order, with whatever directives it
needs - not because a script inferred it from a tag or a naming convention.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, Field


class DocConfigEntry(BaseModel):
    """One documented item within a `ModelGroup` - typically a dbt model name."""

    name: str
    custom: dict[str, Any] = Field(default_factory=dict)


class ModelGroup(BaseModel):
    """`custom` is the same escape hatch as `DocConfigEntry.custom`, at the group
    level - e.g. `elements`, for a group's own intro prose that isn't specific to any
    one model (SAF's "CSCL Source Components for SAF Data", LION's overall
    record-layout overview).
    """

    kind: Literal["model_group"] = "model_group"
    title: str
    entries: list[DocConfigEntry] = Field(default_factory=list)
    custom: dict[str, Any] = Field(default_factory=dict)


class BoilerplateInclude(BaseModel):
    """A pointer to a yaml file of hand-authored `Section`s (see
    `dcpy.utils.doc.load_sections`) - resolving `path` and loading the file is left to
    the caller, since only it knows what `path` is relative to.
    """

    kind: Literal["boilerplate"] = "boilerplate"
    path: str


DocConfigItem = Annotated[ModelGroup | BoilerplateInclude, Field(discriminator="kind")]


class DocConfig(BaseModel):
    items: list[DocConfigItem] = Field(default_factory=list)

    def entry_names(self) -> list[str]:
        """Every dbt model name referenced by a `ModelGroup`, across all items, in
        declared order.
        """
        return [
            entry.name
            for item in self.items
            if isinstance(item, ModelGroup)
            for entry in item.entries
        ]


def load_doc_config(path: Path) -> DocConfig:
    return DocConfig.model_validate(yaml.safe_load(Path(path).read_text()))
