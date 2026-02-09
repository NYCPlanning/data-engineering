import json
import pytest
from pathlib import Path

from dcpy.models import library
from dcpy.models.lifecycle.ingest import ProcessingStep
from dcpy.connectors.ingest_datastore import Connector as IngestDatastoreConnector
from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector, StorageType
from dcpy.lifecycle import connector_registry
from dcpy.lifecycle.ingest.connectors import get_processed_datastore_connector
from dcpy.lifecycle.ingest import validate

from .shared import (
    TEST_OUTPUT,
    RUN_DETAILS,
    INGEST_DEF_DIR,
    TEST_DATASET_NAME,
    DOWNSTREAM_DATASET_1,
)

BASIC_LIBRARY_CONFIG = library.Config(
    dataset=library.DatasetDefinition(
        name=TEST_DATASET_NAME,
        version="20240926",
        acl="public-read",
        source=library.DatasetDefinition.SourceSection(),
        destination=library.DatasetDefinition.DestinationSection(
            geometry=library.GeometryType(SRS="NONE", type="NONE")
        ),
    ),
    execution_details=RUN_DETAILS,
)


@pytest.fixture
def connector(tmp_path):
    conn = IngestDatastoreConnector(
        storage=PathedStorageConnector.from_storage_kwargs(
            conn_type="test_ingest_datastore.local",
            storage_backend=StorageType.LOCAL,
            local_dir=Path(tmp_path),
            # root_folder="datasets",
            _validate_root_path=True,
        )
    )
    connector_registry.connectors.clear()
    connector_registry.connectors.register(conn, conn_type="edm.recipes.datasets")
    yield conn
    connector_registry.connectors.clear()


# TODO
class TestValidateAgainstExistingVersion:
    def test_existing_library(self, connector: IngestDatastoreConnector):
        ds = BASIC_LIBRARY_CONFIG.sparse_dataset

        config_str = json.dumps(BASIC_LIBRARY_CONFIG.model_dump(mode="json"))
        remote_conf_path = connector.storage.storage.root_path / ds.id / ds.version
        remote_conf_path.mkdir(parents=True)
        (remote_conf_path / "config.json").write_text(config_str)

        assert get_processed_datastore_connector().version_exists(ds.id, ds.version), (
            "The version should be found"
        )
        validate.validate_data_against_existing_version(ds.id, ds.version, TEST_OUTPUT)

    def test_existing(self, connector: IngestDatastoreConnector):
        ds = DOWNSTREAM_DATASET_1
        connector.push_versioned(
            key=ds.id, version=ds.version, config=ds, filepath=TEST_OUTPUT
        )
        assert get_processed_datastore_connector().version_exists(ds.id, ds.version)
        validate.validate_data_against_existing_version(ds.id, ds.version, TEST_OUTPUT)

    def test_existing_data_diffs(self, connector: IngestDatastoreConnector):
        ds = DOWNSTREAM_DATASET_1
        connector.push_versioned(
            key=ds.id, version=ds.version, config=ds, filepath=TEST_OUTPUT
        )
        assert get_processed_datastore_connector().version_exists(ds.id, ds.version)
        with pytest.raises(FileExistsError):
            validate.validate_data_against_existing_version(
                ds.id, ds.version, TEST_OUTPUT.parent / "test.parquet"
            )


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


class TestValidatePdDataFrameFunc:
    """transorm._validate_pd_series_func returns dictionary of validation errors"""

    def test_basic(self):
        assert not validate._validate_pd_df_func(function_name="dropna")

    def test_args(self):
        assert not validate._validate_pd_df_func(
            function_name="replace", to_replace="pat", value="repl"
        )

    def test_args_geo(self):
        assert not validate._validate_pd_df_func(
            function_name="replace", to_replace="pat", value="repl", geo=True
        )

    def test_missing_arg(self):
        assert "other" in validate._validate_pd_df_func(function_name="add")

    def test_extra_arg(self):
        assert "extra_arg" in validate._validate_pd_df_func(
            function_name="replace", to_replace="pat", value="repl", extra_arg="foo"
        )

    def test_invalid_function(self):
        res = validate._validate_pd_df_func(function_name="fake_function")
        assert res == "'pd.DataFrame' has no attribute 'fake_function'"

    def test_gpd_without_flag(self):
        res = validate._validate_pd_df_func(function_name="set_crs")
        assert res == "'pd.DataFrame' has no attribute 'set_crs'"

    def test_gpd(self):
        assert not validate._validate_pd_df_func(function_name="set_crs", geo=True)


class TestValidatePdSeriesFunc:
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


class TestValidateFile:
    @pytest.mark.parametrize("ds_id", ["simple", "one_to_many"])
    def test_validate_definition_file_valid(self, ds_id):
        """Test validation of a valid definition file."""
        errors = validate.find_definition_file_validation_errors(ds_id, INGEST_DEF_DIR)
        assert not errors

    @pytest.mark.parametrize(
        ("ds_id", "error"),
        [
            ("invalid_model", "malformatted yml"),
            ("one_to_many_proc_args", "processing steps"),
            ("one_to_many_missing_default", "malformatted yml"),
            ("invalid_source", "source"),
        ],
    )
    def test_invalid_yml(self, ds_id, error):
        """Test validation of an invalid definition file."""
        errors = validate.find_definition_file_validation_errors(ds_id, INGEST_DEF_DIR)
        assert len(errors) == 1
        assert error in errors


def test_validate_definition_folder():
    """Test validation of the ingest_definitions folder."""
    errors = validate.find_definition_folder_validation_errors(INGEST_DEF_DIR)
    # hypothetically an invalid "invalid_definition.yml" in the folder
    assert len(errors) == 5
    assert set(errors.keys()) == {
        "invalid_jinja.yml",
        "invalid_model.yml",
        "invalid_source.yml",
        "one_to_many_proc_args.yml",
        "one_to_many_missing_default.yml",
    }


def test_validate_definition_folder_nonexistent():
    """Test validation of a non-existent folder."""
    with pytest.raises(FileNotFoundError):
        validate.find_definition_folder_validation_errors(Path("nonexistent"))
