from pathlib import Path

import pytest

from dcpy.lifecycle.ingest import plan
from dcpy.models import file
from dcpy.models.lifecycle.ingest import (
    DatasetDefinition,
    DataSourceDefinition,
    Source,
)

from .shared import INGEST_DEF_DIR, RESOLVED, TEST_DATASET_NAME


def _d_path(ds_id: str) -> Path:
    return INGEST_DEF_DIR / f"{ds_id}.yml"


def test_jinja_vars():
    """
    Test `get_jinja_vars` util,
    a pure util for checking expected jinja vars in a string
    """
    no_vars = plan.get_jinja_vars("fake_yml: value")
    assert len(no_vars) == 0, "No variables should have been found"
    vars = plan.get_jinja_vars(r"fake_yml: {{ version }}")
    assert vars == {"version"}, "One var, 'version', should have been found"


class TestReadDefinition:
    """
    Tests configure.read_definition_file
    In addition to ensuring definitions are parsed correctly, catches specific errors around jinja templating
    """

    def test_simple(self):
        definition = plan.read_definition_file(_d_path("simple"))
        assert isinstance(definition, DatasetDefinition)
        assert isinstance(
            definition.ingestion.file_format,
            file.Csv,
        )

    def test_one_to_many(self):
        definition = plan.read_definition_file(_d_path("one_to_many"))
        assert isinstance(definition, DataSourceDefinition)

    def test_jinja(self):
        definition = plan.read_definition_file(_d_path("jinja"), version="test")
        assert isinstance(definition, DatasetDefinition)

    def test_invalid_jinja(self):
        with pytest.raises(
            ValueError,
            match="'version' is only suppored jinja var. Vars in definition: ",
        ):
            plan.read_definition_file(_d_path("invalid_jinja"), version="dummy")


class TestResolveDefinition:
    """
    Tests `resolve_definition` function

    Ensures both simple/1-1/dataset definitions as well as 1-n/"data source" definitions
    are correctly parsed
    """

    def test_file_path_override(self):
        file_path = Path("dir/fake_file_path")
        config = plan.resolve_definition(
            "simple",
            version="24c",
            definition_dir=INGEST_DEF_DIR,
            local_file_path=file_path,
        )
        assert config.source == Source(type="local_file", key=str(file_path))

    def test_simple(self):
        config = plan.resolve_definition("simple", definition_dir=INGEST_DEF_DIR)
        assert len(config.datasets) == 1

    def test_one_to_many(self):
        config = plan.resolve_definition("one_to_many", definition_dir=INGEST_DEF_DIR)
        assert len(config.datasets) == 2
        ds1, ds2 = config.datasets
        assert ds1.id == TEST_DATASET_NAME
        assert ds2.id == "downstream_dataset_2"
        # test defaults get applied correctly
        assert len(ds1.processing_steps) == 1
        assert ds1.processing_steps[0].name == "clean_column_names"
        assert ds1.file_format == file.Geodatabase(
            type="geodatabase", crs="EPSG:4326", layer="downstream_1"
        )
        # final catch-all
        assert config == RESOLVED
