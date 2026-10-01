from __future__ import annotations

from pathlib import Path

import pytest

from dcpy.utils.doc import Doc, Heading, Image, Section, Table, Text
from dcpy.utils.glossary import Glossary, GlossaryEntry, load_glossary


@pytest.fixture
def glossary() -> Glossary:
    return Glossary(
        entries={
            "saf_type_i": GlossaryEntry(
                key="saf_type_i",
                domain="SAF",
                term="SAF Record Type I",
                definition="Named street intersections, keyed by NODEID.",
            ),
            "b5sc": GlossaryEntry(
                key="b5sc",
                domain="General",
                term="B5SC",
                definition='A borough digit plus a 5-digit "street" code.',
            ),
        }
    )


class TestLoadGlossary:
    def test_loads_entries_keyed_by_key(self, tmp_path: Path):
        path = tmp_path / "glossary.csv"
        path.write_text(
            "key,domain,term,definition\n"
            "fic,General,Field Identifier Code (FIC),A short code identifying one field.\n"
        )
        glossary = load_glossary(path)
        assert glossary.entries["fic"].term == "Field Identifier Code (FIC)"
        assert glossary.entries["fic"].domain == "General"


class TestResolveText:
    def test_bare_reference_uses_the_term_as_display_text(self, glossary: Glossary):
        assert glossary.resolve_text("See [[saf_type_i]] for details.") == (
            "See [SAF Record Type I](#glossary-saf_type_i "
            '"Named street intersections, keyed by NODEID.") for details.'
        )

    def test_reference_with_custom_display_text(self, glossary: Glossary):
        assert glossary.resolve_text("types ([[saf_type_i|I]])") == (
            "types ([I](#glossary-saf_type_i "
            '"Named street intersections, keyed by NODEID."))'
        )

    def test_multiple_references_in_one_string(self, glossary: Glossary):
        result = glossary.resolve_text("[[saf_type_i|I]] and [[b5sc]]")
        assert "glossary-saf_type_i" in result
        assert "glossary-b5sc" in result

    def test_text_with_no_references_is_unchanged(self, glossary: Glossary):
        assert (
            glossary.resolve_text("Plain text, no brackets.")
            == "Plain text, no brackets."
        )

    def test_unknown_key_raises(self, glossary: Glossary):
        with pytest.raises(KeyError):
            glossary.resolve_text("[[nonexistent_term]]")

    def test_definition_with_quotes_and_newlines_is_escaped_for_the_tooltip(self):
        glossary = Glossary(
            entries={
                "x": GlossaryEntry(
                    key="x",
                    domain="General",
                    term="X",
                    definition='Has "quotes"\nand a newline.',
                )
            }
        )
        resolved = glossary.resolve_text("[[x]]")
        assert '\\"quotes\\"' in resolved
        assert "\n" not in resolved


class TestResolveInDoc:
    def test_rewrites_text_elements_in_sections_and_subsections(
        self, glossary: Glossary
    ):
        doc = Doc(
            title="D",
            intro=[Text(text="Intro mentions [[b5sc]].")],
            sections=[
                Section(
                    title="Parent",
                    elements=[Text(text="See [[saf_type_i]].")],
                    subsections=[
                        Section(title="Child", elements=[Text(text="Also [[b5sc]].")])
                    ],
                )
            ],
        )
        glossary.resolve_in_doc(doc)
        intro = doc.intro[0]
        section_text = doc.sections[0].elements[0]
        subsection_text = doc.sections[0].subsections[0].elements[0]
        assert isinstance(intro, Text)
        assert isinstance(section_text, Text)
        assert isinstance(subsection_text, Text)
        assert "glossary-b5sc" in intro.text
        assert "glossary-saf_type_i" in section_text.text
        assert "glossary-b5sc" in subsection_text.text

    def test_rewrites_heading_table_cells_and_image_alt_text(self, glossary: Glossary):
        section = Section(
            title="S",
            elements=[
                Heading(text="About [[b5sc]]", level=3),
                Table(headers=["a"], rows=[["mentions [[b5sc]]"]]),
                Image(path="x.png", alt="see [[b5sc]]"),
            ],
        )
        doc = Doc(title="D", sections=[section])
        glossary.resolve_in_doc(doc)
        heading, table, image = section.elements
        assert isinstance(heading, Heading)
        assert isinstance(table, Table)
        assert isinstance(image, Image)
        assert "glossary-b5sc" in heading.text
        assert "glossary-b5sc" in table.rows[0][0]
        assert "glossary-b5sc" in image.alt

    def test_no_references_is_a_no_op(self, glossary: Glossary):
        doc = Doc(
            title="D", sections=[Section(title="S", elements=[Text(text="Plain.")])]
        )
        glossary.resolve_in_doc(doc)
        element = doc.sections[0].elements[0]
        assert isinstance(element, Text)
        assert element.text == "Plain."


class TestSection:
    def test_renders_one_table_sorted_by_domain_then_term(self, glossary: Glossary):
        # "General" sorts before "SAF" even though "B5SC" > "SAF Record Type I"
        # alphabetically - domain is the primary sort key, term only breaks ties
        # within a domain.
        section = glossary.section()
        assert section.title == "Glossary"
        [table] = section.elements
        assert isinstance(table, Table)
        assert table.headers == ["Domain", "Term", "Definition"]
        assert [row[0] for row in table.rows] == ["General", "SAF"]
        assert [row[1] for row in table.rows] == [
            '<a id="glossary-b5sc"></a>B5SC',
            '<a id="glossary-saf_type_i"></a>SAF Record Type I',
        ]

    def test_glossary_link_and_the_table_row_it_points_at_agree(
        self, glossary: Glossary
    ):
        # end-to-end: the anchor a resolved [[key]] link points at must be the exact
        # anchor embedded in the glossary table's own row.
        doc = Doc(
            title="D",
            sections=[Section(title="S", elements=[Text(text="See [[b5sc]].")])],
        )
        doc.sections.append(glossary.section())
        glossary.resolve_in_doc(doc)

        markdown = doc.to_markdown()
        assert '(#glossary-b5sc "A borough digit' in markdown
        assert '<a id="glossary-b5sc"></a>B5SC' in markdown
