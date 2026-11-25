import pytest
from pydantic import ValidationError

from dcpy.lifecycle.ingest import plan, validate
from dcpy.lifecycle.ingest.version_schedules import (
    VersionSchedule,
    geosupport_quarterly_versions,
    is_valid_version,
    versions,
)

from .shared import INGEST_DEF_DIR

QUARTERLY = VersionSchedule.geosupport_quarterly


class TestGeosupportQuarterly:
    def test_one_year(self):
        assert geosupport_quarterly_versions(2026, 2026) == ["26a", "26b", "26c", "26d"]

    def test_range_is_inclusive_and_ordered(self):
        keys = versions(QUARTERLY, 2015, 2027)
        assert keys[0] == "15a"
        assert keys[-1] == "27d"
        assert len(keys) == 13 * 4
        assert keys == sorted(keys)

    def test_single_digit_year_is_padded(self):
        assert geosupport_quarterly_versions(2005, 2005)[0] == "05a"

    def test_backwards_range(self):
        with pytest.raises(ValueError, match="before"):
            geosupport_quarterly_versions(2026, 2025)

    def test_out_of_century(self):
        with pytest.raises(ValueError, match="2000-2099"):
            geosupport_quarterly_versions(1999, 2001)

    @pytest.mark.parametrize("version", ["26a", "26d", "15c", "00b"])
    def test_valid(self, version):
        assert is_valid_version(QUARTERLY, version)

    @pytest.mark.parametrize(
        "version", ["26A", "26e", "2026a", "6a", "26", "26a ", "latest", "20261124"]
    )
    def test_invalid(self, version):
        assert not is_valid_version(QUARTERLY, version)

    def test_generated_keys_are_valid(self):
        assert all(
            is_valid_version(QUARTERLY, v) for v in versions(QUARTERLY, 2015, 2030)
        )


def _with_schedule(tmp_path, ds_id: str, schedule: str):
    """Copy a test definition with a `version_schedule` line added after its id."""
    lines = (INGEST_DEF_DIR / f"{ds_id}.yml").read_text().splitlines(keepends=True)
    path = tmp_path / f"{ds_id}.yml"
    path.write_text(lines[0] + f"version_schedule: {schedule}\n" + "".join(lines[1:]))
    return path


class TestTemplateField:
    def test_default_is_none(self):
        definition = plan.read_definition_file(INGEST_DEF_DIR / "simple.yml")
        assert definition.version_schedule is None

    @pytest.mark.parametrize("ds_id", ["jinja", "one_to_many"])
    def test_parsed(self, tmp_path, ds_id):
        path = _with_schedule(tmp_path, ds_id, "geosupport_quarterly")
        definition = plan.read_definition_file(path, version="26c")
        assert definition.version_schedule == QUARTERLY

    def test_typo_rejected(self, tmp_path):
        path = _with_schedule(tmp_path, "jinja", "geosupport_quartely")
        with pytest.raises(ValidationError, match="version_schedule"):
            plan.read_definition_file(path, version="26c")

    def test_validation_passes_with_placeholder(self, tmp_path):
        _with_schedule(tmp_path, "jinja", "geosupport_quarterly")
        assert not validate.find_version_schedule_error(tmp_path / "jinja.yml")

    def test_validation_needs_placeholder(self, tmp_path):
        _with_schedule(tmp_path, "simple", "geosupport_quarterly")
        errors = validate.find_definition_file_validation_errors("simple", tmp_path)
        assert "{{ version }}" in errors["version_schedule"]
