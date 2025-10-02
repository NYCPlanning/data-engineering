from pathlib import Path
import pytest
from pydantic import BaseModel

from dcpy.models import file
from dcpy.models.lifecycle.ingest.definitions import (
    ProcessingStep,
    Source,
    IngestDefinitionSimple,
    IngestDefinitionOneToMany,
)
from dcpy.lifecycle.ingest import configure

from .shared import INGEST_DEF_DIR, TEST_DATASET_NAME, RESOLVED


def _d_path(ds_id: str) -> Path:
    return INGEST_DEF_DIR / f"{ds_id}.yml"


def test_jinja_vars():
    no_vars = configure.get_jinja_vars("fake_yml: value")
    assert len(no_vars) == 0, "No variables should have been found"
    vars = configure.get_jinja_vars(r"fake_yml: {{ version }}")
    assert vars == {"version"}, "One var, 'version', should have been found"


class TestReadDefinition:
    """
    Tests configure.read_definition
    In addition to ensuring definitions are parsed correctly, catches specific errors around jinja templating
    """

    def test_simple(self):
        definition = configure.read_definition(_d_path("simple"))
        assert isinstance(definition, IngestDefinitionSimple)
        assert isinstance(
            definition.ingestion.file_format,
            file.Csv,
        )

    def test_one_to_many(self):
        definition = configure.read_definition(_d_path("one_to_many"))
        assert isinstance(definition, IngestDefinitionOneToMany)

    def test_jinja(self):
        definition = configure.read_definition(_d_path("jinja"), version="test")
        assert isinstance(definition, IngestDefinitionSimple)

    def test_invalid_jinja(self):
        with pytest.raises(
            ValueError,
            match="'version' is only suppored jinja var. Vars in definition: ",
        ):
            configure.read_definition(_d_path("invalid_jinja"), version="dummy")


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
    errors = configure.find_processing_step_validation_errors("test", [step])
    assert errors == expected_error


class TestValidatePdSeriesFunc:
    """transorm._validate_pd_series_func returns dictionary of validation errors"""

    def test_first_level(self):
        assert not configure._validate_pd_series_func(
            function_name="map", arg={"value 1": "other value 1"}
        )

    def test_str_series(self):
        assert not configure._validate_pd_series_func(
            function_name="str.replace", pat="pat", repl="repl"
        )

    def test_missing_arg(self):
        assert "repl" in configure._validate_pd_series_func(
            function_name="str.replace", pat="pat"
        )

    def test_extra_arg(self):
        assert "extra_arg" in configure._validate_pd_series_func(
            function_name="str.replace", pat="pat", repl="repl", extra_arg="foo"
        )

    def test_invalid_function(self):
        res = configure._validate_pd_series_func(function_name="str.fake_function")
        assert res == "'pd.Series.str' has no attribute 'fake_function'"

    def test_gpd_without_flag(self):
        res = configure._validate_pd_series_func(function_name="force_2d")
        assert res == "'pd.Series' has no attribute 'force_2d'"

    def test_gpd(self):
        assert not configure._validate_pd_series_func(
            function_name="force_2d", geo=True
        )


class SparseBuildMetadata(BaseModel):
    version: str


class TestResolveConfig:
    def test_file_path_override(self):
        file_path = Path("dir/fake_file_path")
        config = configure.resolve_config(
            "simple",
            version="24c",
            definition_dir=INGEST_DEF_DIR,
            local_file_path=file_path,
        )
        assert config.source == Source(type="local_file", key=str(file_path))

    def test_simple(self):
        config = configure.resolve_config("simple", definition_dir=INGEST_DEF_DIR)
        assert len(config.datasets) == 1

    def test_one_to_many(self):
        config = configure.resolve_config("one_to_many", definition_dir=INGEST_DEF_DIR)
        assert len(config.datasets) == 2
        ds1, ds2 = config.datasets
        assert ds1.id == TEST_DATASET_NAME
        assert ds2.id == "downstream_dataset_2"
        # test defaults get applied correctly
        assert len(ds1.processing_steps) == 1
        assert ds1.processing_steps[0].name == "clean_column_names"
        assert ds1.file_format == file.Geodatabase(
            type="geodatabase", crs="EPSG:2263", layer="downstream_1"
        )
        # final catch-all
        assert config == RESOLVED
