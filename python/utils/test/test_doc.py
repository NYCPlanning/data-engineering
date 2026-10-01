from __future__ import annotations

from pathlib import Path

import pytest

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


class TestElementMarkdown:
    def test_heading(self):
        assert Heading(text="Overview", level=3).to_markdown() == "### Overview"

    def test_text(self):
        assert Text(text="Some prose.").to_markdown() == "Some prose."

    def test_table(self):
        table = Table(
            headers=["column", "description"],
            rows=[["widget_id", "Unique id."], ["status", "Processing status."]],
        )
        assert table.to_markdown() == (
            "| column | description |\n"
            "| --- | --- |\n"
            "| widget_id | Unique id. |\n"
            "| status | Processing status. |"
        )

    def test_table_escapes_pipes_and_newlines_in_cells(self):
        table = Table(headers=["a"], rows=[["has | a pipe\nand a newline"]])
        assert table.to_markdown() == (
            "| a |\n| --- |\n| has \\| a pipe<br>and a newline |"
        )

    def test_image(self):
        assert (
            Image(path="diagram.png", alt="a diagram").to_markdown()
            == "![a diagram](diagram.png)"
        )


class TestSection:
    def test_to_markdown_renders_title_as_heading_at_given_level(self):
        section = Section(title="lion_dat_by_field", elements=[Text(text="Body.")])
        assert section.to_markdown(level=2) == (
            '<a id="lion_dat_by_field"></a>\n\n## lion_dat_by_field\n\nBody.'
        )

    def test_to_markdown_emits_an_explicit_anchor_matching_resolved_anchor(self):
        # Not every renderer auto-generates the same heading anchor we compute for the
        # table of contents (case-folding/punctuation handling differ) - an explicit
        # <a id> tag is what actually makes the ToC link work, regardless of renderer.
        section = Section(title="Special Address File")
        assert section.to_markdown().startswith('<a id="special-address-file"></a>')

    def test_subsections_increment_heading_level(self):
        section = Section(
            title="Parent",
            elements=[Text(text="Parent body.")],
            subsections=[
                Section(title="Child", elements=[Text(text="Child body.")]),
            ],
        )
        assert section.to_markdown(level=2) == (
            '<a id="parent"></a>\n\n## Parent\n\nParent body.\n\n'
            '<a id="child"></a>\n\n### Child\n\nChild body.'
        )

    def test_resolved_anchor_defaults_to_slugified_title(self):
        section = Section(title="LION dat by field")
        assert section.resolved_anchor == "lion-dat-by-field"

    def test_resolved_anchor_prefers_explicit_anchor(self):
        section = Section(title="LION dat by field", anchor="custom-anchor")
        assert section.resolved_anchor == "custom-anchor"


class TestDocTableOfContents:
    def test_flattens_sections_and_subsections_depth_first(self):
        doc = Doc(
            title="CSCL Exports",
            sections=[
                Section(
                    title="LION",
                    subsections=[Section(title="lion_dat_by_field")],
                ),
                Section(title="RPL"),
            ],
        )
        assert doc.table_of_contents() == [
            ("LION", "lion", 0),
            ("lion_dat_by_field", "lion_dat_by_field", 1),
            ("RPL", "rpl", 0),
        ]


class TestDocMarkdown:
    def test_full_document(self):
        doc = Doc(
            title="CSCL Exports",
            intro=[Text(text="Generated documentation - do not edit by hand.")],
            sections=[
                Section(
                    title="lion_dat_by_field",
                    elements=[
                        Text(text="One row per LION segment record."),
                        Table(headers=["column"], rows=[["traffic_direction"]]),
                    ],
                )
            ],
        )
        markdown = doc.to_markdown()
        assert markdown.startswith("# CSCL Exports\n\n")
        assert "Generated documentation - do not edit by hand." in markdown
        assert "- [lion_dat_by_field](#lion_dat_by_field)" in markdown
        assert "## lion_dat_by_field" in markdown
        assert "One row per LION segment record." in markdown
        assert "| traffic_direction |" in markdown

    def test_no_sections_omits_table_of_contents(self):
        doc = Doc(title="Empty", intro=[Text(text="Nothing here yet.")])
        markdown = doc.to_markdown()
        assert "Table of Contents" not in markdown
        assert markdown == "# Empty\n\nNothing here yet.\n"


class TestSerializationRoundTrip:
    def test_doc_with_every_element_kind_round_trips_through_json(self):
        doc = Doc(
            title="CSCL Exports",
            intro=[Text(text="Intro.")],
            sections=[
                Section(
                    title="lion_dat_by_field",
                    elements=[
                        Heading(text="Notes", level=3),
                        Text(text="Body."),
                        Table(headers=["a"], rows=[["1"]]),
                        Image(path="diagram.png", alt="a diagram"),
                    ],
                    subsections=[Section(title="Nested")],
                )
            ],
        )
        restored = Doc.model_validate_json(doc.model_dump_json())
        assert restored == doc
        # the discriminated union must reconstruct concrete element subclasses, not a
        # generic dict/base model, or .to_markdown() below would fail
        assert isinstance(restored.sections[0].elements[0], Heading)
        assert isinstance(restored.sections[0].elements[2], Table)
        assert restored.to_markdown() == doc.to_markdown()


class TestLoadSections:
    def test_loads_a_list_of_sections_from_yaml(self, tmp_path: Path):
        path = tmp_path / "boilerplate.yml"
        path.write_text(
            """
            - title: Field Formatting
              elements:
                - kind: text
                  text: Filler fields are always blank.
                - kind: table
                  headers: [Abbreviation, Meaning]
                  rows:
                    - [LJ, Left-justified]
                    - [RJ, Right-justified]
            - title: Introduction
              elements:
                - kind: text
                  text: How this document is organized.
            """
        )
        sections = load_sections(path)
        assert [s.title for s in sections] == ["Field Formatting", "Introduction"]
        assert isinstance(sections[0].elements[1], Table)
        assert sections[0].elements[1].rows == [
            ["LJ", "Left-justified"],
            ["RJ", "Right-justified"],
        ]

    def test_rejects_malformed_yaml_shape(self, tmp_path: Path):
        path = tmp_path / "boilerplate.yml"
        path.write_text("title: not a list of sections")
        with pytest.raises(Exception):
            load_sections(path)


class TestParseElements:
    def test_parses_raw_dicts_into_concrete_element_types(self):
        elements = parse_elements(
            [
                {"kind": "heading", "text": "Notes", "level": 4},
                {"kind": "text", "text": "Some prose."},
                {"kind": "table", "headers": ["a"], "rows": [["1"]]},
                {"kind": "image", "path": "diagram.png", "alt": "a diagram"},
            ]
        )
        assert [type(e) for e in elements] == [Heading, Text, Table, Image]
        assert elements[0].to_markdown() == "#### Notes"

    def test_empty_list_returns_empty_list(self):
        assert parse_elements([]) == []
