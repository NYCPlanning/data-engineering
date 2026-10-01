from __future__ import annotations

from pathlib import Path

import pytest

from dcpy.utils.dbt_project import (
    DbtModelDoc,
    DbtProject,
    DbtProjectCycleError,
    build_doc,
    each_row_is_a_text,
    geospatial_badge_text,
    implementation_details_elements,
    load_dbt_project,
    load_dbt_project_from_manifest,
)
from dcpy.utils.doc import Heading, Section, Table, Text


@pytest.fixture
def sample_project(utils_resources_path) -> DbtProject:
    return load_dbt_project(utils_resources_path / "sample_dbt_project")


@pytest.fixture
def sample_project_from_manifest(utils_resources_path) -> DbtProject:
    # A real `dbt parse` output for the same fixture project (models/macros unchanged;
    # regenerate with `dbt parse --project-dir <sample_dbt_project> --profiles-dir
    # <a profiles.yml with a duckdb target>` if the fixture project changes), with its
    # bulky, irrelevant `macros` section (every built-in dbt macro) emptied out.
    return load_dbt_project_from_manifest(
        utils_resources_path / "sample_dbt_project_manifest.json",
        project_root=utils_resources_path / "sample_dbt_project",
    )


class TestLoadDbtProject:
    def test_discovers_every_model(self, sample_project: DbtProject):
        assert set(sample_project.models) == {
            "stg_widgets",
            "int_widgets_enriched",
            "widgets_export_by_field",
            "widgets_export",
        }

    def test_model_with_schema_yml_gets_description_and_columns(
        self, sample_project: DbtProject
    ):
        model = sample_project.models["stg_widgets"]
        assert model.description == "Raw widgets, lightly renamed for staging."
        assert model.schema_path is not None
        assert model.schema_path.name == "_stg.yml"
        assert [c.name for c in model.columns] == ["widget_id", "widget_name"]
        widget_id = model.column("widget_id")
        assert widget_id is not None
        assert widget_id.description == "Unique widget identifier."

    def test_model_with_no_schema_yml_entry_still_discovered(
        self, sample_project: DbtProject
    ):
        model = sample_project.models["int_widgets_enriched"]
        assert model.description is None
        assert model.schema_path is None
        assert model.columns == []
        # its ref() should still be picked up even with no documentation
        assert model.depends_on == ["stg_widgets"]

    def test_ref_calls_become_depends_on(self, sample_project: DbtProject):
        assert sample_project.models["stg_widgets"].depends_on == []
        assert sample_project.models["int_widgets_enriched"].depends_on == [
            "stg_widgets"
        ]
        assert sample_project.models["widgets_export_by_field"].depends_on == [
            "int_widgets_enriched"
        ]

    def test_ref_hidden_behind_a_macro_argument_is_not_seen(
        self, sample_project: DbtProject
    ):
        # widgets_export.sql pulls from widgets_export_by_field via
        # pull_named_model('widgets_export_by_field'), not a literal ref() - the
        # regex/AST-based parser has no way to see this (it never runs the macro), so
        # the dependency comes back empty. This is the known gap
        # load_dbt_project_from_manifest exists to close - see
        # TestLoadDbtProjectFromManifest below for the same edge done correctly.
        assert sample_project.models["widgets_export"].depends_on == []

    def test_source_calls_are_not_treated_as_refs(self, sample_project: DbtProject):
        # stg_widgets selects from source('raw', 'widgets'), not a ref()
        assert sample_project.models["stg_widgets"].depends_on == []

    def test_tags_from_sql_config_call(self, sample_project: DbtProject):
        model = sample_project.models["widgets_export_by_field"]
        assert model.tags == ["export_doc"]
        assert model.meta == {"group": "widgets"}

    def test_tags_from_yml_config_block(self, sample_project: DbtProject):
        model = sample_project.models["widgets_export"]
        assert model.tags == ["export_doc"]

    def test_each_row_is_a_is_promoted_out_of_meta(self, sample_project: DbtProject):
        model = sample_project.models["widgets_export_by_field"]
        assert model.each_row_is_a == "a widget"
        # popped out, not duplicated
        assert "each_row_is_a" not in model.meta

    def test_each_row_is_a_defaults_to_none(self, sample_project: DbtProject):
        assert sample_project.models["widgets_export"].each_row_is_a is None

    def test_implementation_notes_is_promoted_out_of_meta(
        self, sample_project: DbtProject
    ):
        model = sample_project.models["widgets_export_by_field"]
        assert model.implementation_notes == (
            "Computed by joining the widget catalog to its current price tier."
        )
        assert "implementation_notes" not in model.meta

    def test_implementation_notes_defaults_to_none(self, sample_project: DbtProject):
        assert sample_project.models["widgets_export"].implementation_notes is None

    def test_is_geospatial_is_promoted_out_of_meta(self, sample_project: DbtProject):
        model = sample_project.models["widgets_export_by_field"]
        assert model.is_geospatial is True
        # popped out, not duplicated
        assert "is_geospatial" not in model.meta

    def test_is_geospatial_defaults_to_false(self, sample_project: DbtProject):
        assert sample_project.models["widgets_export"].is_geospatial is False

    def test_column_is_geospatial_is_promoted_out_of_meta(
        self, sample_project: DbtProject
    ):
        column = sample_project.models["widgets_export_by_field"].column("widget_name")
        assert column is not None
        assert column.is_geospatial is True
        assert "is_geospatial" not in column.meta

    def test_column_is_geospatial_defaults_to_false(self, sample_project: DbtProject):
        column = sample_project.models["widgets_export_by_field"].column("widget_id")
        assert column is not None
        assert column.is_geospatial is False

    def test_column_meta_is_preserved(self, sample_project: DbtProject):
        column = sample_project.models["widgets_export_by_field"].column("widget_id")
        assert column is not None
        assert column.meta == {"value_labels": {"": "blank"}}

    def test_doc_block_reference_is_not_expanded(self, sample_project: DbtProject):
        # {{ doc(...) }} is dbt's own docs-block mechanism (see models/product/_docs.md)
        # - this bespoke parser just reads the yml text verbatim, it doesn't render
        # Jinja. Compare against TestLoadDbtProjectFromManifest, which does.
        column = sample_project.models["widgets_export_by_field"].column("widget_id")
        assert column is not None
        assert column.description == '{{ doc("widget_id_doc") }}'

    def test_project_name_from_dbt_project_yml(self, sample_project: DbtProject):
        assert sample_project.name == "sample_project"


class TestFindModels:
    def test_returns_only_tagged_models_in_topological_order(
        self, sample_project: DbtProject
    ):
        # widgets_export's real dependency on widgets_export_by_field is hidden behind
        # a macro (see test_ref_hidden_behind_a_macro_argument_is_not_seen), so the
        # bespoke parser actually gets this ordering backwards - a real consequence of
        # the same gap, not just a missing depends_on entry. Compare against
        # TestLoadDbtProjectFromManifest, which gets the correct order.
        tagged = sample_project.find_models("export_doc")
        assert [m.name for m in tagged] == ["widgets_export", "widgets_export_by_field"]

    def test_no_matches_returns_empty_list(self, sample_project: DbtProject):
        assert sample_project.find_models("nonexistent_tag") == []


class TestTopologicalOrder:
    def test_full_project_order_respects_dependencies(self, sample_project: DbtProject):
        order = sample_project.topological_order()
        assert order.index("stg_widgets") < order.index("int_widgets_enriched")
        assert order.index("int_widgets_enriched") < order.index(
            "widgets_export_by_field"
        )
        # No assertion about widgets_export's position relative to the others: its one
        # real dependency is hidden behind a macro argument, so the bespoke parser has
        # no dependency edge to place it correctly with.

    def test_restricting_to_a_subset_preserves_relative_order(
        self, sample_project: DbtProject
    ):
        # widgets_export_by_field still sorts after stg_widgets, even though
        # int_widgets_enriched (between them in the full graph) isn't in the subset.
        order = sample_project.topological_order(
            {"widgets_export_by_field", "stg_widgets"}
        )
        assert order == ["stg_widgets", "widgets_export_by_field"]

    def test_cycle_raises(self):
        project = DbtProject(
            name="cyclical",
            root_path=Path("/tmp/doesnt-matter"),
            models={
                "a": DbtModelDoc(name="a", sql_path=Path("a.sql"), depends_on=["b"]),
                "b": DbtModelDoc(name="b", sql_path=Path("b.sql"), depends_on=["a"]),
            },
        )
        with pytest.raises(DbtProjectCycleError):
            project.topological_order()


class TestLoadDbtProjectFromManifest:
    """Same fixture project as TestLoadDbtProject, parsed from a real `dbt parse`
    manifest instead of raw files - covering the two things the bespoke parser can't
    do: see a dependency hidden behind a macro argument, and expand a `{{ doc(...) }}`
    block reference.
    """

    def test_discovers_every_model(self, sample_project_from_manifest: DbtProject):
        assert set(sample_project_from_manifest.models) == {
            "stg_widgets",
            "int_widgets_enriched",
            "widgets_export_by_field",
            "widgets_export",
        }

    def test_project_name_from_manifest_metadata(
        self, sample_project_from_manifest: DbtProject
    ):
        assert sample_project_from_manifest.name == "sample_project"

    def test_macro_hidden_ref_is_captured(
        self, sample_project_from_manifest: DbtProject
    ):
        # Unlike the bespoke parser (test_ref_hidden_behind_a_macro_argument_is_not_seen),
        # dbt itself resolved pull_named_model('widgets_export_by_field') at compile
        # time, so the manifest has the real dependency.
        assert sample_project_from_manifest.models["widgets_export"].depends_on == [
            "widgets_export_by_field"
        ]

    def test_doc_block_reference_is_expanded(
        self, sample_project_from_manifest: DbtProject
    ):
        column = sample_project_from_manifest.models["widgets_export_by_field"].column(
            "widget_id"
        )
        assert column is not None
        assert column.description is not None
        assert column.description.startswith("Zero-padded widget identifier")
        assert "{{ doc(" not in column.description

    def test_tagged_models_come_back_in_correct_topological_order(
        self, sample_project_from_manifest: DbtProject
    ):
        # With the real dependency visible, this comes back in the actually-correct
        # order - contrast with TestFindModels.
        # test_returns_only_tagged_models_in_topological_order, which gets it backwards.
        tagged = sample_project_from_manifest.find_models("export_doc")
        assert [m.name for m in tagged] == ["widgets_export_by_field", "widgets_export"]

    def test_columns_and_descriptions_present(
        self, sample_project_from_manifest: DbtProject
    ):
        model = sample_project_from_manifest.models["stg_widgets"]
        assert model.description == "Raw widgets, lightly renamed for staging."
        assert {c.name for c in model.columns} == {"widget_id", "widget_name"}

    def test_column_meta_is_preserved(self, sample_project_from_manifest: DbtProject):
        column = sample_project_from_manifest.models["widgets_export_by_field"].column(
            "widget_id"
        )
        assert column is not None
        assert column.meta == {"value_labels": {"": "blank"}}

    def test_each_row_is_a_is_promoted_out_of_meta(
        self, sample_project_from_manifest: DbtProject
    ):
        model = sample_project_from_manifest.models["widgets_export_by_field"]
        assert model.each_row_is_a == "a widget"
        assert "each_row_is_a" not in model.meta

    def test_implementation_notes_is_promoted_out_of_meta(
        self, sample_project_from_manifest: DbtProject
    ):
        model = sample_project_from_manifest.models["widgets_export_by_field"]
        assert model.implementation_notes == (
            "Computed by joining the widget catalog to its current price tier."
        )
        assert "implementation_notes" not in model.meta

    def test_is_geospatial_is_promoted_out_of_meta(
        self, sample_project_from_manifest: DbtProject
    ):
        model = sample_project_from_manifest.models["widgets_export_by_field"]
        assert model.is_geospatial is True
        assert "is_geospatial" not in model.meta

    def test_is_geospatial_defaults_to_false(
        self, sample_project_from_manifest: DbtProject
    ):
        assert sample_project_from_manifest.models["widgets_export"].is_geospatial is (
            False
        )

    def test_column_is_geospatial_is_promoted_out_of_meta(
        self, sample_project_from_manifest: DbtProject
    ):
        column = sample_project_from_manifest.models["widgets_export_by_field"].column(
            "widget_name"
        )
        assert column is not None
        assert column.is_geospatial is True
        assert "is_geospatial" not in column.meta

    def test_source_call_is_captured_as_a_dependency(
        self, sample_project_from_manifest: DbtProject
    ):
        # stg_widgets selects from source('raw', 'widgets') - the manifest resolves
        # this to the source's table name, unlike the bespoke parser, which only
        # follows ref().
        assert sample_project_from_manifest.models["stg_widgets"].depends_on == [
            "widgets"
        ]

    def test_paths_are_relative_to_project_root(
        self, sample_project_from_manifest: DbtProject
    ):
        model = sample_project_from_manifest.models["stg_widgets"]
        assert model.sql_path == Path("models/staging/stg_widgets.sql")
        assert model.schema_path == Path("models/staging/_stg.yml")


class TestBuildDoc:
    def test_one_section_per_tagged_model_in_topological_order(
        self, sample_project_from_manifest: DbtProject
    ):
        doc = build_doc(
            sample_project_from_manifest, "export_doc", title="Sample Project"
        )
        assert doc.title == "Sample Project"
        assert [s.title for s in doc.sections] == [
            "widgets_export_by_field",
            "widgets_export",
        ]

    def test_section_gets_description_as_text_and_documented_columns_as_a_table(
        self, sample_project_from_manifest: DbtProject
    ):
        doc = build_doc(
            sample_project_from_manifest, "export_doc", title="Sample Project"
        )
        section = doc.sections[0]
        assert section.title == "widgets_export_by_field"

        text_elements = [e for e in section.elements if isinstance(e, Text)]
        assert text_elements[0].text == "**Each row is a:** a widget"
        assert text_elements[1].text == "🌐 **Geospatially determined**"
        assert text_elements[2].text == (
            "Widgets formatted for export, one row per widget."
        )

        [table] = [e for e in section.elements if isinstance(e, Table)]
        assert table.headers == ["column", "description", "values"]
        rows_by_column = {row[0]: row for row in table.rows}
        # widget_id's description comes from the {{ doc(...) }} block, expanded because
        # this is built from the manifest-based project
        assert rows_by_column["widget_id"][1].startswith(
            "Zero-padded widget identifier"
        )
        assert rows_by_column["widget_id"][2] == "`` blank"
        # widget_name is flagged is_geospatial - its Field-cell name is icon-prefixed
        assert "widget_name" not in rows_by_column
        assert "🌐 widget_name" in rows_by_column
        assert "status" in rows_by_column

    def test_undocumented_model_still_gets_a_section_with_no_elements(self):
        # A tagged model with no description and no documented columns should degrade
        # gracefully to an empty section, not an empty/broken Text or Table element.
        undocumented = DbtModelDoc(
            name="mystery_model",
            sql_path=Path("mystery_model.sql"),
            tags=["export_doc"],
        )
        project = DbtProject(
            name="p",
            root_path=Path("/tmp/doesnt-matter"),
            models={"mystery_model": undocumented},
        )
        doc = build_doc(project, "export_doc", title="Sample Project")
        assert doc.sections == [Section(title="mystery_model")]

    def test_rendered_markdown_is_well_formed(
        self, sample_project_from_manifest: DbtProject
    ):
        doc = build_doc(
            sample_project_from_manifest, "export_doc", title="Sample Project"
        )
        markdown = doc.to_markdown()
        assert markdown.startswith("# Sample Project")
        assert "## widgets_export_by_field" in markdown
        assert "## widgets_export" in markdown
        assert "| widget_id |" in markdown
        assert "**Each row is a:** a widget" in markdown
        assert "🌐 **Geospatially determined**" in markdown
        assert "#### Implementation Details" in markdown
        assert "Computed by joining the widget catalog" in markdown


class TestEachRowIsAText:
    def test_renders_a_bolded_lead_in_line(self):
        model = DbtModelDoc(name="m", sql_path=Path("m.sql"), each_row_is_a="a widget")
        text = each_row_is_a_text(model)
        assert text is not None
        assert text.to_markdown() == "**Each row is a:** a widget"

    def test_none_when_not_set(self):
        model = DbtModelDoc(name="m", sql_path=Path("m.sql"))
        assert each_row_is_a_text(model) is None


class TestGeospatialBadgeText:
    def test_renders_a_badge_line(self):
        model = DbtModelDoc(name="m", sql_path=Path("m.sql"), is_geospatial=True)
        text = geospatial_badge_text(model)
        assert text is not None
        assert text.to_markdown() == "🌐 **Geospatially determined**"

    def test_custom_label(self):
        model = DbtModelDoc(name="m", sql_path=Path("m.sql"), is_geospatial=True)
        text = geospatial_badge_text(model, label="Spatially assigned")
        assert text is not None
        assert text.to_markdown() == "🌐 **Spatially assigned**"

    def test_none_when_not_set(self):
        model = DbtModelDoc(name="m", sql_path=Path("m.sql"))
        assert geospatial_badge_text(model) is None


class TestImplementationDetailsElements:
    def test_renders_a_heading_and_text(self):
        model = DbtModelDoc(
            name="m",
            sql_path=Path("m.sql"),
            implementation_notes="Joined to a price tier lookup.",
        )
        elements = implementation_details_elements(model)
        assert elements == [
            Heading(text="Implementation Details", level=4),
            Text(text="Joined to a price tier lookup."),
        ]

    def test_custom_heading_level(self):
        model = DbtModelDoc(
            name="m", sql_path=Path("m.sql"), implementation_notes="Notes."
        )
        [heading, _] = implementation_details_elements(model, heading_level=2)
        assert heading == Heading(text="Implementation Details", level=2)

    def test_empty_list_when_not_set(self):
        model = DbtModelDoc(name="m", sql_path=Path("m.sql"))
        assert implementation_details_elements(model) == []
