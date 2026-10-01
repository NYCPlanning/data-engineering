"""Two ways to parse a dbt project's model/column documentation into a common shape
(`DbtProject` / `DbtModelDoc` / `DbtColumnDoc`):

- `load_dbt_project` - a bespoke, dependency-light parser that reads schema yml files
  and regex/AST-scans each model's raw SQL for `ref()` calls and `config()` kwargs. It
  doesn't invoke dbt, needs no database connection or rendered `profiles.yml`, and works
  on a project that's never been `dbt deps`/`dbt parse`d. The tradeoff: it only
  understands `ref()` calls and `config()` kwargs written as static literals in the
  model being scanned - a dependency picked up through a macro argument (rather than a
  literal `ref()` in that model's own SQL) or a `{{ doc(...) }}` block reference won't
  be seen or expanded.
- `load_dbt_project_from_manifest` - reads an already-generated `manifest.json` (the
  output of any dbt invocation - `dbt parse`, `dbt compile`, `dbt build`, ...) via the
  `dbt-artifacts-parser` package. This reflects the fully Jinja-rendered project, so it
  gets both of the above right, at the cost of needing a manifest to already exist.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any, Iterable

import yaml
from dbt_artifacts_parser.parser import parse_manifest
from pydantic import BaseModel, Field

from dcpy.utils.doc import Doc, Heading, Image, Section, Table, Text

_REF_CALL_PATTERN = re.compile(r"\bref\(([^()]*)\)")
_QUOTED_STRING_PATTERN = re.compile(r"""['"]([^'"]+)['"]""")
_CONFIG_CALL_START_PATTERN = re.compile(r"\bconfig\s*\(")


class DbtColumnDoc(BaseModel):
    """A dbt model column, as declared in a `_*.yml` schema file. `is_geospatial` -
    see `DbtModelDoc.is_geospatial` - can be set per column when only some of a
    model's columns are geometry-dependent, for a more precise signal than flagging
    the whole model.
    """

    name: str
    data_type: str | None = None
    description: str | None = None
    is_geospatial: bool = False
    meta: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class DbtModelDoc(BaseModel):
    """A dbt model, assembled from its `.sql` file and (if present) its schema yml
    entry. `sql_path`/`schema_path` are relative to the project root.

    `each_row_is_a` is a short noun phrase for what one row represents (e.g. "City
    Street") - the same convention already used in our open-data product-metadata
    (`product-metadata/products/*/*/metadata.yml`), promoted to a first-class field
    here. `implementation_notes` is deliberately separate from `description`: the
    latter is what a row *means* (business-facing, safe to read with zero pipeline
    context); the former is *how it's computed* (joins, filters, algorithms) - keeping
    them apart, rather than one paragraph mixing both, is what makes it possible to
    render the technical half under its own clearly-labeled heading instead of
    silently burying mechanism inside what's supposed to be a plain-language summary.
    `is_geospatial` flags a model whose values depend on geometry - a spatial join
    (containment, intersection, proximity) or a geometric computation (ordering by
    position, clipping) - as opposed to plain attribute/key joins. Worth knowing at a
    glance: geometry-dependent derivations carry a different class of risk (precision,
    boundary/overlap edge cases, coordinate-system issues) than an ordinary join does.

    All three are read from `meta` (`meta.each_row_is_a` / `meta.implementation_notes`
    / `meta.is_geospatial`) and popped out once read, so none is duplicated - a plain
    top-level yml key wouldn't work here, since dbt silently drops unrecognized
    model-level keys before they reach `manifest.json`, breaking
    `load_dbt_project_from_manifest`; `meta` is the one place that survives.
    """

    name: str
    sql_path: Path
    schema_path: Path | None = None
    description: str | None = None
    each_row_is_a: str | None = None
    implementation_notes: str | None = None
    is_geospatial: bool = False
    meta: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    columns: list[DbtColumnDoc] = Field(default_factory=list)
    depends_on: list[str] = Field(default_factory=list)

    def column(self, name: str) -> DbtColumnDoc | None:
        return next((c for c in self.columns if c.name == name), None)


class DbtProjectCycleError(Exception):
    """Raised when a dbt project's ref() graph contains a cycle."""


class DbtProject(BaseModel):
    """A parsed dbt project: every model found under its model-paths, keyed by name."""

    name: str
    root_path: Path
    models: dict[str, DbtModelDoc]

    def find_models(self, tag: str) -> list[DbtModelDoc]:
        """Models carrying the given tag, in the project's topological
        (upstream-first) order.
        """
        tagged = {name for name, model in self.models.items() if tag in model.tags}
        return [self.models[name] for name in self.topological_order(tagged)]

    def topological_order(self, model_names: Iterable[str] | None = None) -> list[str]:
        """Model names in dependency order (a model always appears after everything it
        `ref()`s, transitively). Restricting to `model_names` filters the full project
        order rather than recomputing a subgraph, so a selected model's position still
        reflects its complete upstream dependency chain - not just dependencies among
        the selected models.
        """
        full_order = self._full_topological_order()
        if model_names is None:
            return full_order
        wanted = set(model_names)
        return [name for name in full_order if name in wanted]

    def _full_topological_order(self) -> list[str]:
        visited: dict[str, bool] = {}  # False = in progress (on the stack), True = done
        order: list[str] = []

        def visit(name: str, stack: tuple[str, ...]) -> None:
            if name not in self.models:
                return  # a ref() to a seed, or something outside this project
            state = visited.get(name)
            if state is True:
                return
            if state is False:
                cycle = " -> ".join([*stack, name])
                raise DbtProjectCycleError(
                    f"Cycle detected in dbt ref() graph: {cycle}"
                )
            visited[name] = False
            for dep in self.models[name].depends_on:
                visit(dep, (*stack, name))
            visited[name] = True
            order.append(name)

        for model_name in sorted(self.models):
            visit(model_name, ())
        return order


def load_dbt_project(project_dir: Path) -> DbtProject:
    """Parse a dbt project rooted at `project_dir` (the directory containing its
    `dbt_project.yml`)."""
    project_dir = Path(project_dir)
    project_config = yaml.safe_load((project_dir / "dbt_project.yml").read_text()) or {}
    model_dirs: list[str] = project_config.get("model-paths") or ["models"]

    sql_paths: dict[str, Path] = {
        sql_path.stem: sql_path
        for model_dir in model_dirs
        for sql_path in sorted((project_dir / model_dir).rglob("*.sql"))
    }

    schema_entries: dict[str, tuple[dict[str, Any], Path]] = {}
    for model_dir in model_dirs:
        for yml_path in sorted((project_dir / model_dir).rglob("*.yml")):
            content = yaml.safe_load(yml_path.read_text()) or {}
            for entry in content.get("models") or []:
                schema_entries[entry["name"]] = (entry, yml_path)

    models: dict[str, DbtModelDoc] = {}
    for model_name, sql_path in sql_paths.items():
        sql_text = sql_path.read_text()
        sql_config = _extract_config_kwargs(sql_text)
        entry, schema_path = schema_entries.get(model_name, ({}, None))
        yml_config = entry.get("config") or {}

        columns = []
        for col in entry.get("columns") or []:
            col_meta = dict(col.get("meta") or {})
            columns.append(
                DbtColumnDoc(
                    name=col["name"],
                    data_type=col.get("data_type"),
                    description=col.get("description"),
                    is_geospatial=col_meta.pop("is_geospatial", False),
                    meta=col_meta,
                    tags=_as_list(col.get("tags")),
                )
            )

        merged_meta = _merge_meta(
            sql_config.get("meta"), entry.get("meta"), yml_config.get("meta")
        )
        each_row_is_a = merged_meta.pop("each_row_is_a", None)
        implementation_notes = merged_meta.pop("implementation_notes", None)
        is_geospatial = merged_meta.pop("is_geospatial", False)

        other_model_names = sql_paths.keys() - {model_name}
        models[model_name] = DbtModelDoc(
            name=model_name,
            sql_path=sql_path.relative_to(project_dir),
            schema_path=schema_path.relative_to(project_dir) if schema_path else None,
            description=entry.get("description"),
            each_row_is_a=each_row_is_a,
            implementation_notes=implementation_notes,
            is_geospatial=is_geospatial,
            meta=merged_meta,
            tags=_merge_tags(
                sql_config.get("tags"), entry.get("tags"), yml_config.get("tags")
            ),
            columns=columns,
            depends_on=sorted(_extract_refs(sql_text) & other_model_names),
        )

    return DbtProject(name=project_config["name"], root_path=project_dir, models=models)


def load_dbt_project_from_manifest(
    manifest_path: Path, project_root: Path | None = None
) -> DbtProject:
    """Parse a dbt project from an already-generated `manifest.json`. Unlike
    `load_dbt_project`, descriptions/tags/meta reflect the fully Jinja-rendered project
    (`{{ doc(...) }}` blocks included), and `depends_on` reflects every `ref()`/`source()`
    dbt actually resolved - including ones reached through a macro argument rather than
    written literally in the model's own SQL.

    `project_root` defaults to `manifest_path.parent.parent`, matching dbt's standard
    `<project>/target/manifest.json` layout; pass it explicitly if the manifest lives
    somewhere else.
    """
    manifest_path = Path(manifest_path)
    manifest = parse_manifest(manifest=json.loads(manifest_path.read_text()))
    # Not every historical manifest schema version's ManifestMetadata declares
    # project_name (mypy sees the full union across all of them) - it's been present
    # since dbt's early manifest versions in practice, but fall back to getattr rather
    # than assert the type away.
    project_name = getattr(manifest.metadata, "project_name", None)
    if not project_name:
        raise ValueError(f"{manifest_path} has no project_name in its metadata")

    def resolve_name(unique_id: str) -> str:
        if unique_id in manifest.nodes:
            return manifest.nodes[unique_id].name
        if unique_id in manifest.sources:
            return manifest.sources[unique_id].name
        return unique_id

    models: dict[str, DbtModelDoc] = {}
    for node in manifest.nodes.values():
        if node.resource_type != "model" or node.package_name != project_name:
            continue

        columns = []
        for column in (node.columns or {}).values():
            col_meta = dict(column.meta or {})
            columns.append(
                DbtColumnDoc(
                    name=column.name,
                    data_type=column.data_type,
                    description=column.description or None,
                    is_geospatial=col_meta.pop("is_geospatial", False),
                    meta=col_meta,
                    tags=list(column.tags or []),
                )
            )

        schema_path = None
        if node.patch_path:
            # "<package_name>://<path relative to project root>"
            schema_path = Path(node.patch_path.split("://", 1)[-1])

        meta = dict(node.meta or {})
        each_row_is_a = meta.pop("each_row_is_a", None)
        implementation_notes = meta.pop("implementation_notes", None)
        is_geospatial = meta.pop("is_geospatial", False)

        models[node.name] = DbtModelDoc(
            name=node.name,
            sql_path=Path(node.original_file_path),
            schema_path=schema_path,
            description=node.description or None,
            each_row_is_a=each_row_is_a,
            implementation_notes=implementation_notes,
            is_geospatial=is_geospatial,
            meta=meta,
            tags=list(node.tags or []),
            columns=columns,
            depends_on=sorted(
                {
                    resolve_name(dep)
                    for dep in (node.depends_on.nodes if node.depends_on else None)
                    or []
                }
            ),
        )

    return DbtProject(
        name=project_name,
        root_path=project_root or manifest_path.parent.parent,
        models=models,
    )


def documented_columns_table(model: DbtModelDoc) -> Table | None:
    """A plain column table (name/description/value labels) for whichever of `model`'s
    columns have a description - `None` if none do. Reused by `build_doc` and by
    products that assemble their own `Doc` from a `DocConfig` instead.
    """
    documented = [column for column in model.columns if column.description]
    if not documented:
        return None

    rows = []
    for column in documented:
        value_labels = column.meta.get("value_labels")
        values = (
            "; ".join(f"`{value}` {label}" for value, label in value_labels.items())
            if value_labels
            else ""
        )
        name = f"🌐 {column.name}" if column.is_geospatial else column.name
        rows.append([name, column.description or "", values])
    return Table(headers=["column", "description", "values"], rows=rows)


def each_row_is_a_text(model: DbtModelDoc) -> Text | None:
    """A bolded "Each row is a: <phrase>" lead-in line, if `model.each_row_is_a` is
    set - `None` otherwise. Meant to be a section's first element, before its
    description. Reused by `build_doc` and by products assembling their own `Doc`
    from a `DocConfig`.
    """
    if not model.each_row_is_a:
        return None
    return Text(text=f"**Each row is a:** {model.each_row_is_a}")


def geospatial_badge_text(
    model: DbtModelDoc, label: str = "Geospatially determined"
) -> Text | None:
    """A small "🌐 **<label>**" badge line if `model.is_geospatial` is set - `None`
    otherwise. Meant to sit alongside `each_row_is_a_text` near the top of a section,
    flagging that the model's values depend on a spatial join or geometric computation
    rather than plain attribute/key joins. Deliberately not glossary-aware: pass a
    `[[glossary_key|...]]`-style label if the caller wants it linked.
    """
    if not model.is_geospatial:
        return None
    return Text(text=f"🌐 **{label}**")


def implementation_details_elements(
    model: DbtModelDoc, heading_level: int = 4
) -> list[Heading | Text]:
    """A "#### Implementation Details" heading plus `model.implementation_notes`, if
    set - an empty list otherwise. Meant to be appended after a section's main
    description/table, keeping "what this is" (description) visibly separate from
    "how it's computed" (implementation_notes) rather than one paragraph blending
    both. Reused by `build_doc` and by products assembling their own `Doc` from a
    `DocConfig`.
    """
    if not model.implementation_notes:
        return []
    return [
        Heading(text="Implementation Details", level=heading_level),
        Text(text=model.implementation_notes),
    ]


def build_doc(project: DbtProject, tag: str, title: str) -> Doc:
    """A first-draft `Doc`: one `Section` per model carrying `tag` (in the project's
    topological order - see `DbtProject.find_models`), with an "Each row is a" lead-in
    (if set), the model's description as a `Text` element, a
    `documented_columns_table`, and an "Implementation Details" block (if
    `implementation_notes` is set). A caller with more specific needs - different
    table shapes per model, images, explicit grouping/ordering - is expected to build
    its `Doc` directly from a `DocConfig` instead; see CSCL's own generate_docs.py for
    an example.
    """
    sections = []
    for model in project.find_models(tag):
        elements: list[Heading | Text | Table | Image] = []

        each_row_is_a = each_row_is_a_text(model)
        if each_row_is_a is not None:
            elements.append(each_row_is_a)

        geospatial_badge = geospatial_badge_text(model)
        if geospatial_badge is not None:
            elements.append(geospatial_badge)

        if model.description:
            elements.append(Text(text=model.description))

        table = documented_columns_table(model)
        if table is not None:
            elements.append(table)

        elements.extend(implementation_details_elements(model))

        sections.append(Section(title=model.name, elements=elements))

    return Doc(title=title, sections=sections)


def _as_list(value: str | list[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


def _merge_tags(*sources: str | list[str] | None) -> list[str]:
    tags: list[str] = []
    for source in sources:
        for tag in _as_list(source):
            if tag not in tags:
                tags.append(tag)
    return tags


def _merge_meta(*sources: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for source in sources:
        if source:
            merged.update(source)
    return merged


def _extract_refs(sql_text: str) -> set[str]:
    """Model names this model `ref()`s. For the two-arg cross-project form
    `ref('package', 'model')`, only the model name (the last quoted argument) is kept.
    """
    refs: set[str] = set()
    for call_args in _REF_CALL_PATTERN.findall(sql_text):
        quoted = _QUOTED_STRING_PATTERN.findall(call_args)
        if quoted:
            refs.add(quoted[-1])
    return refs


def _find_balanced_parens(text: str, open_idx: int) -> str | None:
    """Given `open_idx` just after an opening `(`, return the text up to its matching
    `)`, treating parens inside quoted strings as inert.
    """
    depth = 1
    i = open_idx
    in_quote: str | None = None
    while i < len(text):
        ch = text[i]
        if in_quote:
            if ch == "\\":
                i += 2
                continue
            if ch == in_quote:
                in_quote = None
        elif ch in ("'", '"'):
            in_quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return text[open_idx:i]
        i += 1
    return None


def _extract_config_kwargs(sql_text: str) -> dict[str, Any]:
    """Best-effort parse of a model's `{{ config(...) }}` call into its kwargs, for the
    keys we care about (`tags`, `meta`). Only static literals are understood - a value
    built up with Jinja logic is silently dropped, not evaluated.
    """
    match = _CONFIG_CALL_START_PATTERN.search(sql_text)
    if not match:
        return {}
    body = _find_balanced_parens(sql_text, match.end())
    if body is None:
        return {}
    try:
        parsed = ast.parse(f"__config__({body})", mode="eval")
    except SyntaxError:
        return {}
    call = parsed.body
    if not isinstance(call, ast.Call):
        return {}
    kwargs: dict[str, Any] = {}
    for keyword in call.keywords:
        if keyword.arg is None:
            continue
        try:
            kwargs[keyword.arg] = ast.literal_eval(keyword.value)
        except (ValueError, SyntaxError):
            continue
    return kwargs
