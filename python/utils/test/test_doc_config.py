from __future__ import annotations

from pathlib import Path

from dcpy.utils.doc_config import (
    BoilerplateInclude,
    DocConfig,
    ModelGroup,
    load_doc_config,
)


class TestLoadDocConfig:
    def test_loads_model_groups_and_entries_with_custom_directives(
        self, tmp_path: Path
    ):
        path = tmp_path / "doc_config.yml"
        path.write_text(
            """
            items:
              - kind: model_group
                title: LION
                entries:
                  - name: lion_dat_by_field
                    custom:
                      table: dat_fields
              - kind: model_group
                title: Normalizing Tables
                entries:
                  - name: enders_by_field
                    custom:
                      table: columns
            """
        )
        config = load_doc_config(path)
        titles = [item.title for item in config.items if isinstance(item, ModelGroup)]
        assert titles == ["LION", "Normalizing Tables"]
        first = config.items[0]
        assert isinstance(first, ModelGroup)
        assert first.entries[0].name == "lion_dat_by_field"
        assert first.entries[0].custom == {"table": "dat_fields"}

    def test_model_group_can_carry_its_own_custom_directives(self, tmp_path: Path):
        # A group's own intro prose - not specific to any one model - goes here (e.g.
        # SAF's "CSCL Source Components for SAF Data").
        path = tmp_path / "doc_config.yml"
        path.write_text(
            """
            items:
              - kind: model_group
                title: Special Address File
                custom:
                  elements:
                    - kind: text
                      text: Shared background on SAF's source data.
                entries:
                  - name: saf_i_by_field
            """
        )
        config = load_doc_config(path)
        group = config.items[0]
        assert isinstance(group, ModelGroup)
        assert group.custom == {
            "elements": [
                {"kind": "text", "text": "Shared background on SAF's source data."}
            ]
        }

    def test_entry_with_no_custom_defaults_to_empty_dict(self, tmp_path: Path):
        path = tmp_path / "doc_config.yml"
        path.write_text(
            """
            items:
              - kind: model_group
                title: Face Code
                entries:
                  - name: face_code_by_field
            """
        )
        config = load_doc_config(path)
        assert isinstance(config.items[0], ModelGroup)
        assert config.items[0].entries[0].custom == {}

    def test_boilerplate_include_is_just_a_path_pointer(self, tmp_path: Path):
        path = tmp_path / "doc_config.yml"
        path.write_text(
            """
            items:
              - kind: boilerplate
                path: docs/boilerplate/field_formatting.yml
            """
        )
        config = load_doc_config(path)
        [item] = config.items
        assert isinstance(item, BoilerplateInclude)
        assert item.path == "docs/boilerplate/field_formatting.yml"

    def test_boilerplate_and_model_groups_interleave_in_declared_order(
        self, tmp_path: Path
    ):
        # This is the whole point of a single `items` list: an intro can sit before
        # every model group and an appendix after all of them, purely by position.
        path = tmp_path / "doc_config.yml"
        path.write_text(
            """
            items:
              - kind: boilerplate
                path: intro.yml
              - kind: model_group
                title: LION
                entries:
                  - name: lion_dat_by_field
              - kind: boilerplate
                path: appendix.yml
            """
        )
        config = load_doc_config(path)
        kinds = [item.kind for item in config.items]
        assert kinds == ["boilerplate", "model_group", "boilerplate"]


class TestEntryNames:
    def test_flattens_every_model_group_in_order_and_skips_boilerplate(self):
        config = DocConfig.model_validate(
            {
                "items": [
                    {"kind": "boilerplate", "path": "intro.yml"},
                    {
                        "kind": "model_group",
                        "title": "A",
                        "entries": [{"name": "a1"}, {"name": "a2"}],
                    },
                    {"kind": "model_group", "title": "B", "entries": [{"name": "b1"}]},
                    {"kind": "boilerplate", "path": "appendix.yml"},
                ]
            }
        )
        assert config.entry_names() == ["a1", "a2", "b1"]

    def test_empty_config_has_no_entry_names(self):
        assert DocConfig().entry_names() == []
