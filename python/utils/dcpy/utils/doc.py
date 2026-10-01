"""A small, format-agnostic document model: a `Doc` is a title, some intro content,
and a tree of `Section`s; each `Section` holds a list of typed elements (`Heading`,
`Text`, `Table`, `Image`) plus its own nested `subsections`.

The point of modeling this as data (rather than writing markdown directly, the way
libraries like mdutils do) is that a producer - `dcpy.utils.dbt_project.build_doc`, say
- can hand back a `Doc`, and a caller can walk and *mutate* it (e.g. add columns to a
`Table`'s rows) before anyone renders anything. Because every piece is a pydantic model
with a `kind` discriminator, a `Doc` also round-trips through `model_dump()`/
`model_validate()` - JSON or yaml - with no extra code.

`to_markdown()` is the only renderer today. A `.docx`/`.pdf` export belongs at a higher
layer (dcpy-product-metadata already has a Jinja/HTML/PDF pipeline in `writers/`) rather
than pulling that kind of dependency into this foundational package.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, Field, TypeAdapter

_SLUG_STRIP_PATTERN = re.compile(r"[^\w\s-]")
_SLUG_WHITESPACE_PATTERN = re.compile(r"[\s]+")


def _slugify(text: str) -> str:
    """A best-effort approximation of GitHub's heading-anchor algorithm, so a
    `[text](#anchor)` link in a table of contents resolves when the markdown is viewed
    on GitHub (or most other GFM renderers).
    """
    slug = _SLUG_STRIP_PATTERN.sub("", text.lower())
    return _SLUG_WHITESPACE_PATTERN.sub("-", slug.strip())


def _escape_cell(text: str) -> str:
    return text.replace("\n", "<br>").replace("|", r"\|")


class Heading(BaseModel):
    kind: Literal["heading"] = "heading"
    text: str
    level: int = 2

    def to_markdown(self) -> str:
        return f"{'#' * self.level} {self.text}"


class Text(BaseModel):
    kind: Literal["text"] = "text"
    text: str

    def to_markdown(self) -> str:
        return self.text


class Table(BaseModel):
    kind: Literal["table"] = "table"
    headers: list[str]
    rows: list[list[str]]

    def to_markdown(self) -> str:
        header_row = "| " + " | ".join(self.headers) + " |"
        separator_row = "| " + " | ".join("---" for _ in self.headers) + " |"
        data_rows = [
            "| " + " | ".join(_escape_cell(cell) for cell in row) + " |"
            for row in self.rows
        ]
        return "\n".join([header_row, separator_row, *data_rows])


class Image(BaseModel):
    kind: Literal["image"] = "image"
    path: str
    alt: str = ""

    def to_markdown(self) -> str:
        return f"![{self.alt}]({self.path})"


DocElement = Annotated[Heading | Text | Table | Image, Field(discriminator="kind")]


class Section(BaseModel):
    title: str
    anchor: str | None = None
    elements: list[DocElement] = Field(default_factory=list)
    subsections: list[Section] = Field(default_factory=list)

    @property
    def resolved_anchor(self) -> str:
        return self.anchor or _slugify(self.title)

    def to_markdown(self, level: int = 2) -> str:
        # An explicit anchor, rather than relying on the table of contents' slug
        # matching whatever a given renderer auto-generates for the heading text -
        # those disagree often enough (case-folding, punctuation handling) that a
        # multi-word title's ToC link can silently 404 in one viewer and work in
        # another. This guarantees the two always agree, since the ToC links here.
        anchor = f'<a id="{self.resolved_anchor}"></a>'
        heading = f"{'#' * level} {self.title}"
        parts = [f"{anchor}\n\n{heading}"]
        parts.extend(element.to_markdown() for element in self.elements)
        parts.extend(
            subsection.to_markdown(level=level + 1) for subsection in self.subsections
        )
        return "\n\n".join(parts)


Section.model_rebuild()


class Doc(BaseModel):
    title: str
    intro: list[DocElement] = Field(default_factory=list)
    sections: list[Section] = Field(default_factory=list)

    def table_of_contents(self) -> list[tuple[str, str, int]]:
        """Every section's (title, anchor, depth), depth-first, depth starting at 0 for
        top-level sections. Derived from `sections` rather than stored separately, so
        it can't drift out of sync with the sections themselves.
        """
        entries: list[tuple[str, str, int]] = []

        def visit(section: Section, depth: int) -> None:
            entries.append((section.title, section.resolved_anchor, depth))
            for subsection in section.subsections:
                visit(subsection, depth + 1)

        for section in self.sections:
            visit(section, 0)
        return entries

    def to_markdown(self) -> str:
        parts = [f"# {self.title}"]
        parts.extend(element.to_markdown() for element in self.intro)
        if self.sections:
            toc_lines = [
                f"{'  ' * depth}- [{title}](#{anchor})"
                for title, anchor, depth in self.table_of_contents()
            ]
            parts.append("\n".join(toc_lines))
        parts.extend(section.to_markdown(level=2) for section in self.sections)
        return "\n\n".join(parts) + "\n"


_SECTIONS_ADAPTER = TypeAdapter(list[Section])
_ELEMENTS_ADAPTER = TypeAdapter(list[DocElement])


def load_sections(path: Path) -> list[Section]:
    """Load hand-authored `Section`s (e.g. boilerplate content with no natural home on
    any one model - an intro, a shared glossary) from a yaml file whose top level is a
    list of `Section` objects. A caller typically prepends or interleaves the result
    with generated sections (e.g. from `dcpy.utils.dbt_project.build_doc`) before
    rendering.
    """
    return _SECTIONS_ADAPTER.validate_python(yaml.safe_load(Path(path).read_text()))


def parse_elements(raw: list[dict[str, Any]]) -> list[Heading | Text | Table | Image]:
    """Validate a list of raw element dicts - e.g. an ad hoc `custom.elements`
    directive in a `dcpy.utils.doc_config.DocConfigEntry` - into concrete `Heading` /
    `Text` / `Table` / `Image` objects, the same discriminated-union parsing
    `load_sections` uses internally for a whole `Section`. Lets a product's script
    attach arbitrary extra prose or sub-headings to a generated section (a "Notes"
    block after a table, a methodology sub-heading) without writing new Python for
    each case.
    """
    return _ELEMENTS_ADAPTER.validate_python(raw)
