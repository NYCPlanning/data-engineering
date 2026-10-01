#!/usr/bin/env python3
"""
Generate CSCL's export documentation from the dbt project - a proof of concept for
replacing docs/ETL_V8_02012024.md with something generated off the dbt models rather
than hand-maintained prose.

Assembly is entirely driven by doc_config.yml (see dcpy.utils.doc_config.DocConfig):
item order (model groups and boilerplate includes, interleaved however they're
declared), model order within a group, and per-model directives (which table shape,
if any; extra prose/headings; images) all live there, not in this script. This script
only knows how to *interpret* those directives for CSCL specifically - a `dat_fields`
table means "join this model's text_formatting__* seed"; another product's generator
would give its own custom directives their own meaning.

Requires target/manifest.json to already exist (run `dbt parse`, or any other dbt
invocation, first) - see dcpy.utils.dbt_project.load_dbt_project_from_manifest.

Usage:
    python scripts/generate_docs.py
"""

import csv
import os
import re
import sys
from pathlib import Path

from dcpy.utils.dbt_project import (
    DbtModelDoc,
    DbtProject,
    documented_columns_table,
    each_row_is_a_text,
    geospatial_badge_text,
    implementation_details_elements,
    load_dbt_project_from_manifest,
)
from dcpy.utils.doc import (
    Doc,
    Heading,
    Image,
    Section,
    Table,
    Text,
    load_sections,
    parse_elements,
)
from dcpy.utils.doc_config import (
    BoilerplateInclude,
    DocConfig,
    DocConfigEntry,
    ModelGroup,
    load_doc_config,
)
from dcpy.utils.glossary import load_glossary

PRODUCT_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = PRODUCT_ROOT / "target" / "manifest.json"
DOC_CONFIG_PATH = PRODUCT_ROOT / "doc_config.yml"
GLOSSARY_PATH = PRODUCT_ROOT / "glossary.csv"
SEEDS_DIR = PRODUCT_ROOT / "seeds" / "text_formatting"
OUTPUT_PATH = PRODUCT_ROOT / "output" / "docs" / "cscl_exports.md"

EXPORT_DOC_TAG = "export_doc"


def _warn_on_drift(project: DbtProject, config: DocConfig) -> None:
    """doc_config.yml's entries and the export_doc dbt tag are two independent
    declarations of "this model is documented" - warn (don't yet fail, see Q10 in the
    docs-generation scoping notes) if they disagree, rather than silently trusting one.
    """
    tagged = {model.name for model in project.find_models(EXPORT_DOC_TAG)}
    configured = set(config.entry_names())

    only_tagged = sorted(tagged - configured)
    if only_tagged:
        print(
            f"  ! tagged `{EXPORT_DOC_TAG}` but missing from doc_config.yml: {only_tagged}"
        )

    only_configured = sorted(configured - tagged)
    if only_configured:
        print(
            f"  ! in doc_config.yml but not tagged `{EXPORT_DOC_TAG}`: {only_configured}"
        )


# dbt model names (our own internal implementation detail) must never appear anywhere
# in the generated doc - not just as a section title (display_name already handles
# that), but nowhere in any rendered prose either, description and Implementation
# Details alike. "Describe the mechanism, don't name the file" - e.g. "via a Roadbed
# Pointer List lookup", not "int__saf_segments.sql looks up...".
_INTERNAL_REFERENCE_PATTERN = re.compile(r"\b(int|stg)__\w+|_by_field\b|\.sql\b")


def _warn_on_internal_references(text: str, where: str) -> None:
    match = _INTERNAL_REFERENCE_PATTERN.search(text)
    if match:
        print(
            f"  ! {where} references a dbt model by name ({match.group(0)!r}) - reword it"
        )


def _read_formatting_seed(seed_name: str) -> list[dict[str, str]]:
    with open(SEEDS_DIR / f"{seed_name}.csv") as f:
        return list(csv.DictReader(f))


def _dat_fields_table(model: DbtModelDoc) -> Table:
    """Every field in `model`'s text_formatting__* seed, in on-the-wire order, with
    description/values filled in from documented columns and left blank otherwise.
    """
    seed_name = next(
        (dep for dep in model.depends_on if dep.startswith("text_formatting__")), None
    )
    if seed_name is None:
        raise ValueError(
            f"{model.name}: doc_config.yml asks for a dat_fields table, but this model "
            "doesn't depend on a text_formatting__* seed"
        )

    undocumented = []
    rows = []
    for field in _read_formatting_seed(seed_name):
        column = model.column(field["field_name"])
        description = column.description if column else None
        if description is None and not field["field_name"].startswith("filler_"):
            undocumented.append(field["field_name"])

        value_labels = (column.meta.get("value_labels") if column else None) or {}
        values = "; ".join(
            f"`{value}` {label}" for value, label in value_labels.items()
        )

        field_name = (
            f"🌐 {field['field_name']}"
            if column is not None and column.is_geospatial
            else field["field_name"]
        )
        rows.append(
            [
                # column is named "fic" in most seeds, but "field_number" in lion_dat's
                field.get("fic") or field.get("field_number") or "",
                field_name,
                (description or "").replace("\n", " "),
                field["field_length"],
                f"{field['start_index']}-{field['end_index']}",
                field["justify_and_fill"],
                values,
            ]
        )

    if undocumented:
        print(
            f"  ! {model.name}: {len(undocumented)} undocumented field(s): "
            f"{', '.join(undocumented)}"
        )

    return Table(
        headers=[
            "FIC",
            "Field",
            "Description",
            "Length",
            "Position",
            "Format",
            "Values",
        ],
        rows=rows,
    )


def _build_section(
    project: DbtProject, entry: DocConfigEntry, group_label: str
) -> Section:
    model = project.models.get(entry.name)
    if model is None:
        raise ValueError(f"doc_config.yml references unknown model {entry.name!r}")

    for text, field in [
        (model.description, "description"),
        (model.each_row_is_a, "each_row_is_a"),
        (model.implementation_notes, "implementation_notes"),
    ]:
        if text:
            _warn_on_internal_references(text, f"{entry.name}.{field}")

    elements: list[Heading | Text | Table | Image] = []
    each_row_is_a = each_row_is_a_text(model)
    if each_row_is_a is not None:
        elements.append(each_row_is_a)

    geospatial_badge = geospatial_badge_text(
        model, label="[[geospatial_join|Geospatially determined]]"
    )
    if geospatial_badge is not None:
        elements.append(geospatial_badge)

    if model.description:
        elements.append(Text(text=model.description))

    table_type = entry.custom.get("table")
    if table_type == "dat_fields":
        elements.append(_dat_fields_table(model))
    elif table_type == "columns":
        table = documented_columns_table(model)
        if table is not None:
            elements.append(table)
    elif table_type is not None:
        raise ValueError(f"{entry.name}: unknown doc_config table type {table_type!r}")

    for image in entry.custom.get("images", []):
        elements.append(Image(**image))

    # The stark overview/mechanism split: description above is "what this is" -
    # business-facing, safe to read without any pipeline context. Implementation
    # Details is "how it's computed" - joins, filters, algorithms - clearly
    # demarcated under its own heading rather than blended into the description.
    elements.extend(implementation_details_elements(model))

    # An escape hatch for anything that doesn't fit the vocabulary above - authored
    # directly as Doc elements in doc_config.yml rather than requiring new Python for
    # each case.
    elements.extend(parse_elements(entry.custom.get("elements", [])))

    # The dbt model name is an internal implementation detail - the doc should show
    # the actual deliverable file name a reader would recognize (declared explicitly
    # in doc_config.yml, since the mapping isn't always 1:1 - e.g. one *_by_field
    # model can fan out into five borough files), plus its enclosing group for
    # context (multiple files can otherwise look identical out of context, e.g. every
    # SAF file's title alone doesn't say it's a SAF file).
    display_name = entry.custom.get("display_name")
    if display_name is None:
        raise ValueError(
            f"{entry.name}: doc_config.yml entry has no custom.display_name - the "
            "dbt model name must not be exposed as a section title"
        )

    return Section(title=f"{display_name} ({group_label})", elements=elements)


def _build_doc_from_config(project: DbtProject, config: DocConfig, title: str) -> Doc:
    sections: list[Section] = []
    for item in config.items:
        if isinstance(item, ModelGroup):
            group_label = item.custom.get("short_title", item.title)
            group_elements = parse_elements(item.custom.get("elements", []))
            for element in group_elements:
                if isinstance(element, Text):
                    _warn_on_internal_references(
                        element.text, f"{item.title} (group intro)"
                    )
            sections.append(
                Section(
                    title=item.title,
                    elements=group_elements,
                    subsections=[
                        _build_section(project, entry, group_label)
                        for entry in item.entries
                    ],
                )
            )
        elif isinstance(item, BoilerplateInclude):
            sections.extend(load_sections(PRODUCT_ROOT / item.path))
        else:
            raise ValueError(f"Unknown doc_config item kind: {item!r}")

    return Doc(title=title, sections=sections)


def _resolve_image_paths(doc: Doc, output_dir: Path) -> None:
    """Image.path values are authored relative to the product root (see
    media/README.md) - rewrite them relative to wherever this doc is actually written.
    """
    prefix = Path(os.path.relpath(PRODUCT_ROOT, start=output_dir))

    def rewrite(elements: list) -> None:
        for element in elements:
            if isinstance(element, Image):
                element.path = str(prefix / element.path)

    def walk(section: Section) -> None:
        rewrite(section.elements)
        for subsection in section.subsections:
            walk(subsection)

    rewrite(doc.intro)
    for section in doc.sections:
        walk(section)


def main() -> None:
    if not MANIFEST_PATH.exists():
        print(f"✗ {MANIFEST_PATH} not found - run `dbt parse` first", file=sys.stderr)
        sys.exit(1)

    print(f"Loading dbt project from {MANIFEST_PATH}...")
    project = load_dbt_project_from_manifest(MANIFEST_PATH, project_root=PRODUCT_ROOT)

    print(f"Loading doc_config from {DOC_CONFIG_PATH}...")
    config = load_doc_config(DOC_CONFIG_PATH)
    _warn_on_drift(project, config)

    print("Assembling doc from doc_config.yml...")
    doc = _build_doc_from_config(project, config, title="CSCL Exports")

    print(f"Loading glossary from {GLOSSARY_PATH}...")
    glossary = load_glossary(GLOSSARY_PATH)
    doc.sections.append(glossary.section())
    # After appending the glossary section, so a [[key]] inside a definition can
    # reference another term too.
    glossary.resolve_in_doc(doc)

    _resolve_image_paths(doc, OUTPUT_PATH.parent)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(doc.to_markdown())
    print(f"✓ Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
