import pytest
from pathlib import Path
from unittest import TestCase

from dcpy.models.lifecycle.ingest import ProcessingStep
from dcpy.lifecycle.ingest import validate


INGEST_TEMPLATES = Path(__file__).parent / "resources" / "templates"


def test_validate_template_file_valid():
    """Test validation of a valid template file."""
    valid_file = INGEST_TEMPLATES / "dcp_addresspoints.yml"
    validate.validate_template_file(valid_file)  # Should not raise


def test_validate_template_file_invalid():
    """Test validation of an invalid template file."""
    invalid_file = INGEST_TEMPLATES / "invalid_template.yml"
    with pytest.raises(Exception):
        validate.validate_template_file(invalid_file)


def test_validate_template_folder():
    """Test validation of the ingest_templates folder."""
    errors = validate.validate_template_folder(INGEST_TEMPLATES)
    # hypothetically an invalid "invalid_template.yml" in the folder
    assert len(errors) == 1
    assert any("invalid_template.yml" in error for error in errors)


def test_validate_template_folder_nonexistent():
    """Test validation of a non-existent folder."""
    errors = validate.validate_template_folder(Path("nonexistent"))
    assert len(errors) == 1
    assert "doesn't exist" in errors[0]


@pytest.mark.parametrize(
    ("step", "expected_error"),
    [
        # No Error
        (
            ProcessingStep(name="drop_columns", args={"columns": [0]}),
            {},
        ),
        # Non-existent function
        (
            ProcessingStep(name="fake_function_name"),
            {"fake_function_name": "Function not found"},
        ),
        # Missing arg
        (
            ProcessingStep(name="drop_columns", args={}),
            {"drop_columns": {"columns": "Missing"}},
        ),
        # Unexpected arg
        (
            ProcessingStep(name="drop_columns", args={"columns": [0], "fake_arg": 0}),
            {"drop_columns": {"fake_arg": "Unexpected"}},
        ),
        # Invalid pd series func
        (
            ProcessingStep(
                name="pd_series_func",
                args={"function_name": "str.fake_function", "column_name": "_"},
            ),
            {"pd_series_func": "'pd.Series.str' has no attribute 'fake_function'"},
        ),
    ],
)
def test_find_processing_step_validation_errors_errors(step, expected_error):
    errors = validate.find_processing_step_validation_errors("test", [step])
    assert errors == expected_error


class TestValidatePdSeriesFunc(TestCase):
    """transorm._validate_pd_series_func returns dictionary of validation errors"""

    def test_first_level(self):
        assert not validate._validate_pd_series_func(
            function_name="map", arg={"value 1": "other value 1"}
        )

    def test_str_series(self):
        assert not validate._validate_pd_series_func(
            function_name="str.replace", pat="pat", repl="repl"
        )

    def test_missing_arg(self):
        assert "repl" in validate._validate_pd_series_func(
            function_name="str.replace", pat="pat"
        )

    def test_extra_arg(self):
        assert "extra_arg" in validate._validate_pd_series_func(
            function_name="str.replace", pat="pat", repl="repl", extra_arg="foo"
        )

    def test_invalid_function(self):
        res = validate._validate_pd_series_func(function_name="str.fake_function")
        assert res == "'pd.Series.str' has no attribute 'fake_function'"

    def test_gpd_without_flag(self):
        res = validate._validate_pd_series_func(function_name="force_2d")
        assert res == "'pd.Series' has no attribute 'force_2d'"

    def test_gpd(self):
        assert not validate._validate_pd_series_func(function_name="force_2d", geo=True)
