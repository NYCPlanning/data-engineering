"""A glossary: short-key terms with a domain, a definition, loaded from a CSV,
rendered as one `Doc` table (sorted by domain, then term), and referenced inline from
anywhere else in a `Doc` via `[[key]]` or `[[key|display text]]` - resolved into a
markdown link to that term's own row, carrying its full definition as a native HTML
hover tooltip (markdown's link-title syntax: `[text](url "title")`), so a reader
doesn't have to scroll down to understand a term.

Deliberately explicit, not auto-detected: matching prose substrings against every
glossary term (e.g. a bare "A" for a term named "SAF Record Type A") would
false-positive constantly. A `[[key]]` reference only ever appears where a human put
one.

A markdown table row can't be an anchor target on its own, so each row's Term cell
carries an invisible `<a id="...">` right before the term text - raw inline HTML is
valid inside a GFM table cell, so this still gives every entry its own deep link
without needing a whole section per term.

Don't put a `[[key]]` reference inside a glossary entry's own `definition`: that text
also becomes the hover tooltip for every *other* entry that references this one, and a
tooltip is plain HTML `title` text - a nested markdown link there can't resolve or
render, it just leaks the raw `[[key]]` syntax. Write the other term's name as plain
text instead; it's already a glossary entry in its own right; `[[key]]` references
belong in prose elsewhere in the doc, not inside a definition.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

from pydantic import BaseModel, Field

from dcpy.utils.doc import Doc, Heading, Image, Section, Table, Text

_REFERENCE_PATTERN = re.compile(r"\[\[([^\]|]+)(?:\|([^\]]+))?\]\]")


class GlossaryEntry(BaseModel):
    key: str
    domain: str
    term: str
    definition: str

    @property
    def anchor(self) -> str:
        # Namespaced so a term can't collide with an unrelated model/group Section
        # that happens to slugify to the same anchor.
        return f"glossary-{self.key}"


class Glossary(BaseModel):
    entries: dict[str, GlossaryEntry] = Field(default_factory=dict)

    def resolve_text(self, text: str) -> str:
        """Replace every `[[key]]`/`[[key|display]]` reference in `text` with a
        markdown link to that entry's anchor, its definition as the hover tooltip.
        Raises if a reference names a key that isn't in this glossary - a broken
        reference is a typo to fix, not something to degrade gracefully around.
        """

        def replace(match: re.Match[str]) -> str:
            key = match.group(1).strip()
            display = (match.group(2) or "").strip()
            entry = self.entries.get(key)
            if entry is None:
                raise KeyError(
                    f"[[{key}]] references a glossary term that doesn't exist"
                )
            label = display or entry.term
            tooltip = entry.definition.replace("\n", " ").replace('"', '\\"')
            return f'[{label}](#{entry.anchor} "{tooltip}")'

        return _REFERENCE_PATTERN.sub(replace, text)

    def resolve_in_doc(self, doc: Doc) -> None:
        """Rewrite every `[[key]]` reference found anywhere in `doc` - its intro,
        every section/subsection's `Text`/`Heading`/`Table`/`Image` content - in
        place. Call this after assembling the full `Doc` (including, if you want
        glossary entries to be able to reference each other, after appending
        `self.section()` to it).
        """

        def resolve_element(element: Heading | Text | Table | Image) -> None:
            if isinstance(element, (Text, Heading)):
                element.text = self.resolve_text(element.text)
            elif isinstance(element, Table):
                element.rows = [
                    [self.resolve_text(cell) for cell in row] for row in element.rows
                ]
            elif isinstance(element, Image):
                element.alt = self.resolve_text(element.alt)

        def walk(section: Section) -> None:
            for element in section.elements:
                resolve_element(element)
            for subsection in section.subsections:
                walk(subsection)

        for element in doc.intro:
            resolve_element(element)
        for section in doc.sections:
            walk(section)

    def section(self, title: str = "Glossary") -> Section:
        """A `Section` with one table, sorted by domain then term - what a `[[key]]`
        reference's link actually points at (each row's Term cell carries its own
        anchor).
        """
        ordered = sorted(
            self.entries.values(), key=lambda e: (e.domain.lower(), e.term.lower())
        )
        table = Table(
            headers=["Domain", "Term", "Definition"],
            rows=[
                [
                    entry.domain,
                    f'<a id="{entry.anchor}"></a>{entry.term}',
                    entry.definition,
                ]
                for entry in ordered
            ],
        )
        return Section(title=title, elements=[table])


def load_glossary(path: Path) -> Glossary:
    """Load a glossary from a CSV with columns `key,domain,term,definition`."""
    with open(path, newline="") as f:
        entries = {row["key"]: GlossaryEntry(**row) for row in csv.DictReader(f)}
    return Glossary(entries=entries)
