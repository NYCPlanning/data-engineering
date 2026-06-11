import os
import tempfile
from datetime import date, datetime
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from dcpy.connectors.edm.models import BuildKey, DatasetType, PublishKey
from dcpy.connectors.registry import ConnectorRegistry, VersionedConnector
from dcpy.lifecycle import connector_registry
from dcpy.lifecycle.builds import plan
from dcpy.lifecycle.builds.models import InputDataset, StageConfigValue
from dcpy.test.lifecycle.builds.conftest import (
    REQUIRED_VERSION_ENV_VAR,
    RESOURCES_DIR,
    set_mock_published_connector,
)
from dcpy.utils import versions

RECIPE_PATH = RESOURCES_DIR / "recipe.yml"
RECIPE_NO_DEFAULTS_PATH = RESOURCES_DIR / "recipe_no_defaults.yml"
RECIPE_NO_VERSION_PATH = RESOURCES_DIR / "recipe_no_version.yml"
RECIPE_W_VERSION_TYPE = RESOURCES_DIR / "recipe_w_version_type.yml"
RECIPE_W_MULTIPLE_SOURCES = RESOURCES_DIR / "recipe_edm_custom.yml"
RECIPE_W_STAGES = RESOURCES_DIR / "recipe_w_stages.yml"
RECIPE_W_UNRESOLVABLE_STAGES = RESOURCES_DIR / "recipe_w_unresolvable_var.yml"
BUILD_METADATA_PATH = RESOURCES_DIR / "build_metadata.json"
SOURCE_VERSIONS_PATH = RESOURCES_DIR / "source_data_versions.csv"

PRODUCT = "Tester"
MOCKED_LATEST_VERSION = "v1"
MOCKED_DATASET_NAME = "Agency Dataset"


@pytest.fixture(autouse=True)
def set_mock_recipes_connector_defaults():
    set_mock_recipes_connector(
        method_responses={
            "get_latest_version": MOCKED_LATEST_VERSION,
            "get_name": MOCKED_DATASET_NAME,
        }
    )
    # Also mock the published connector to avoid S3 access during VERSION_PREV lookup
    set_mock_published_connector({"list_versions": []})
    # TODO... we should probably at some point figure out a default set of
    # connectors that we can reset to after this fixture yields


def set_mock_recipes_connector(
    method_responses: dict[str, str], conn_type="edm.recipes.datasets"
):
    connector_registry.connectors.clear()

    edm_recipes_mock = MagicMock(spec=VersionedConnector)
    edm_recipes_mock.conn_type = conn_type
    for method, response in method_responses.items():
        setattr(edm_recipes_mock, method, Mock(return_value=response))
    connector_registry.connectors.register(edm_recipes_mock, conn_type=conn_type)
    assert 1 == len(connector_registry.connectors.list_registered())


def add_required_version_var_to_env():
    os.environ[REQUIRED_VERSION_ENV_VAR] = "v123"


def test_resolve_dataset_fails_without_version():
    ds_id = "test"
    ds = InputDataset(id=ds_id)
    with pytest.raises(Exception, match=f"Dataset '{ds_id}' requires version"):
        ds.dataset


class TestVersionStrategies(TestCase):
    recipe = plan.recipe_from_yaml(RECIPE_PATH)
    recipe.version = None

    def test_no_version_strategy_fails(self):
        self.recipe.version_strategy = None
        with pytest.raises(Exception, match="No version or version_strategy provided"):
            plan.resolve_version(self.recipe)

    def test_today(self):
        self.recipe.version_strategy = versions.SimpleVersionStrategy.today
        assert plan.resolve_version(self.recipe) == versions.generate_today().label

    def test_first_of_month(self):
        self.recipe.version_strategy = versions.SimpleVersionStrategy.first_of_month
        assert (
            plan.resolve_version(self.recipe)
            == versions.generate_first_of_month().label
        )

    def test_simple_bump_latest_release(self):
        set_mock_published_connector({"get_latest_version": "24v1"})
        self.recipe.version_strategy = (
            versions.SimpleVersionStrategy.bump_latest_release
        )
        assert plan.resolve_version(self.recipe) == "24v2"

    def test_bump_latest_release(self):
        set_mock_published_connector({"get_latest_version": "24v1"})
        self.recipe.version_strategy = versions.BumpLatestRelease(bump_latest_release=2)
        assert plan.resolve_version(self.recipe) == "24v3"

    def test_pin_to_source_dataset(self):
        version = "20240101"
        set_mock_recipes_connector(
            conn_type="edm.recipes.datasets",
            method_responses={"get_latest_version": version},
        )
        self.recipe.version_strategy = versions.PinToSourceDataset(
            pin_to_source_dataset="has_no_version_or_type"
        )
        assert plan.resolve_version(self.recipe) == version

    def test_pin_to_source_dataset_explicit(self):
        source_dataset = "has_pinned_version"
        self.recipe.version_strategy = versions.PinToSourceDataset(
            pin_to_source_dataset=source_dataset
        )
        source_dataset_record = self.recipe.inputs.datasets[1]
        assert source_dataset_record.id == source_dataset, (
            "test setup error - check order of source datasets in recipe.yml"
        )
        assert plan.resolve_version(self.recipe) == source_dataset_record.version

    def test_pin_to_source_dataset_version_env_var(self):
        self.recipe.version_strategy = versions.PinToSourceDataset(
            pin_to_source_dataset="has_version_from_env"
        )
        with pytest.raises(ValueError, match="To use 'pin to source dataset'"):
            plan.resolve_version(self.recipe)

    def test_pin_to_source_dataset_missing_dataset(self):
        dataset = "fake_dataset"
        self.recipe.version_strategy = versions.PinToSourceDataset(
            pin_to_source_dataset=dataset
        )
        with pytest.raises(
            ValueError, match=f"Cannot pin build version to dataset '{dataset}'"
        ):
            plan.resolve_version(self.recipe)


@pytest.mark.usefixtures("file_setup_teardown")
@pytest.mark.usefixtures("create_buckets")
class TestRecipesWithDefaults(TestCase):
    def test_plan_recipe_failing_env_var(self):
        """One of the datasets requires a REQUIRED_VERSION_ENV_VAR environment variable for the version.
        Plan should fail since no variable is present.
        """
        os.environ.pop(REQUIRED_VERSION_ENV_VAR)
        with self.assertRaises(Exception) as e:
            plan.plan(RECIPE_PATH)
            assert REQUIRED_VERSION_ENV_VAR in str(e.exception)

    def test_provide_manual_version(self):
        add_required_version_var_to_env()
        version = "test_version"
        planned = plan.plan_recipe(RECIPE_PATH, version=version)
        assert planned.version == version
        assert planned.is_resolved, "Dataset is not resolved"

    def test_plan_recipe_defaults(self):
        """Tests that defaults are set correctly when a recipe is planned."""
        add_required_version_var_to_env()
        planned = plan.plan_recipe(RECIPE_PATH)

        missing_source = [ds for ds in planned.inputs.datasets if not ds.source]
        assert not missing_source

        had_no_version_or_type = [
            ds for ds in planned.inputs.datasets if ds.id == "has_no_version_or_type"
        ][0]

        assert had_no_version_or_type.version == MOCKED_LATEST_VERSION, (
            f"The missing version strategy should be applied to find the latest version for this dataset. Found {had_no_version_or_type.version}"
        )
        assert had_no_version_or_type.file_type == DatasetType.csv, (
            "The datatype should default to a csv, as specified in the dataset_defaults"
        )
        assert planned.is_resolved, "Dataset is not resolved"

    def test_input_datasets(self):
        add_required_version_var_to_env()
        planned = plan.plan_recipe(RECIPE_PATH)
        datasets = planned.inputs.datasets

        assert datasets[0].id == "has_version_from_env"
        assert datasets[0].source == "edm.recipes.datasets"
        assert datasets[0].name == MOCKED_DATASET_NAME


@pytest.mark.usefixtures("create_buckets")
class TestRecipesWithNoVersion(TestCase):
    def test_no_provided_recipe_fails(self):
        with pytest.raises(Exception, match="No version or version_strategy provided"):
            plan.plan_recipe(RECIPE_NO_VERSION_PATH)

    def test_provide_manual_version(self):
        add_required_version_var_to_env()
        version = "test_version"
        planned = plan.plan_recipe(RECIPE_NO_VERSION_PATH, version=version)
        assert planned.version == version
        assert planned.is_resolved, "Dataset is not resolved"


@pytest.mark.usefixtures("file_setup_teardown")
@pytest.mark.usefixtures("create_buckets")
@patch("dcpy.connectors.edm.recipes.get_file_types")
class TestRecipesNoDefaults(TestCase):
    def test_plan_recipe_default_type(self, get_file_types):
        """Tests that default type is pg_dump if found when not otherwise specified."""
        add_required_version_var_to_env()
        get_file_types.return_value = {
            DatasetType.pg_dump,
            DatasetType.parquet,
        }
        planned = plan.plan_recipe(RECIPE_NO_DEFAULTS_PATH)
        assert planned.inputs.datasets[0].file_type == DatasetType.pg_dump
        assert planned.is_resolved, "Dataset is not resolved"

    def test_serializing_and_deserializing(self, get_file_types):
        """Deserializing python models is a minefield."""
        get_file_types.return_value = {
            DatasetType.pg_dump,
            DatasetType.parquet,
        }
        lock_file = plan.plan(RECIPE_NO_DEFAULTS_PATH)
        plan.recipe_from_yaml(lock_file)

    @patch("dcpy.connectors.edm.recipes.get_archive_date")
    def test_plan_recipe_populates_archive_date(
        self, mock_archive_date, get_file_types
    ):
        get_file_types.return_value = {DatasetType.pg_dump}
        mock_archive_date.return_value = datetime(2024, 6, 1, 12, 0, 0)
        planned = plan.plan_recipe(RECIPE_NO_DEFAULTS_PATH)
        assert planned.inputs.datasets[0].archive_date == date(2024, 6, 1)


@pytest.mark.usefixtures("create_buckets")
class TestRecipeVars(TestCase):
    def test_version_type_var_is_absent(self):
        """Ensures VERSION_TYPE is absent in recipe 'vars' attribute when version_type is None."""
        add_required_version_var_to_env()
        version = "test_version"
        planned = plan.plan_recipe(RECIPE_PATH, version=version)
        assert planned.version_type is None  # sanity check
        assert "VERSION_TYPE" not in planned.vars

    def test_version_type_is_present(self):
        """Test that the version_type is set correctly and matches env 'VERSION_TYPE' variable."""
        version = "test_version"
        planned = plan.plan_recipe(RECIPE_W_VERSION_TYPE, version)
        assert planned.version_type is not None  # sanity check
        assert planned.version_type == planned.vars["VERSION_TYPE"], (
            "version_type mismatch with recipe.vars"
        )
        assert planned.vars["VERSION_TYPE"] == os.environ["VERSION_TYPE"], (
            "'version_type' recipe variable mismatch with 'VERSION_TYPE' env variable"
        )


def build_metadata_exists(key, file):
    return file == "build_metadata.json"


def source_versions_csv_exists(key, file):
    return file == "source_data_versions.csv"


@pytest.mark.usefixtures("create_buckets")
class TestRepeat(TestCase):
    def test_repeat_published_version_from_source_data_versions(self):
        import shutil
        import tempfile

        version = "21v1"
        recipe = plan.recipe_from_yaml(RECIPE_NO_DEFAULTS_PATH)

        # Create test CSV
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            test_csv = tmp_path / "source_data_versions.csv"
            shutil.copy(SOURCE_VERSIONS_PATH, test_csv)

            def mock_resource_exists(key, version, resource_path, **kw):
                return resource_path == "source_data_versions.csv"

            def mock_pull(key, version, destination_path, filepath=None, **kw):
                if filepath == "source_data_versions.csv":
                    shutil.copy(test_csv, destination_path)
                    return {"path": destination_path}
                raise FileNotFoundError()

            set_mock_published_connector(
                {
                    "resource_exists": Mock(side_effect=mock_resource_exists),
                    "pull_versioned": Mock(side_effect=mock_pull),
                }
            )

            repeat_file = plan.repeat_build(
                PublishKey(product=recipe.name, version=version),
                recipe_file=RECIPE_NO_DEFAULTS_PATH,
            )
            repeat = plan.recipe_from_yaml(repeat_file)
            assert repeat.is_resolved
            assert repeat.version == version
            assert repeat.inputs.datasets[0].version == "v1"

    def test_repeat_build_from_source_data_versions(self):
        import shutil
        import tempfile

        recipe = plan.recipe_from_yaml(RECIPE_NO_DEFAULTS_PATH)

        # Create test CSV
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            test_csv = tmp_path / "source_data_versions.csv"
            shutil.copy(SOURCE_VERSIONS_PATH, test_csv)

            def mock_resource_exists(key, version, resource_path, **kw):
                return resource_path == "source_data_versions.csv"

            def mock_pull(key, version, destination_path, filepath=None, **kw):
                if filepath == "source_data_versions.csv":
                    shutil.copy(test_csv, destination_path)
                    return {"path": destination_path}
                raise FileNotFoundError()

            set_mock_published_connector(
                {
                    "resource_exists": Mock(side_effect=mock_resource_exists),
                    "pull_versioned": Mock(side_effect=mock_pull),
                }
            )

            repeat_file = plan.repeat_build(
                BuildKey(product=recipe.name, build=recipe.version),
                recipe_file=RECIPE_NO_DEFAULTS_PATH,
                manual_version="21v1",
            )
            repeat = plan.recipe_from_yaml(repeat_file)
            assert repeat.is_resolved
            assert repeat.version == "21v1"
            assert repeat.inputs.datasets[0].version == "v1"

    def test_repeat_build_from_source_data_versions_fails_without_version(self):
        recipe = plan.recipe_from_yaml(RECIPE_NO_DEFAULTS_PATH)

        def mock_resource_exists(key, version, resource_path, **kw):
            return resource_path == "source_data_versions.csv"

        set_mock_published_connector(
            {"resource_exists": Mock(side_effect=mock_resource_exists)}
        )

        with pytest.raises(
            ValueError,
            match="Version must be supplied manually if repeating an older build without build_metadata.json",
        ):
            plan.repeat_build(
                BuildKey(product=recipe.name, build=recipe.version),
                recipe_file=RECIPE_NO_DEFAULTS_PATH,
            )

    def test_repeat_build_from_source_data_versions_fails_without_recipe_path(self):
        recipe = plan.recipe_from_yaml(RECIPE_NO_DEFAULTS_PATH)

        def mock_resource_exists(key, version, resource_path, **kw):
            return resource_path == "source_data_versions.csv"

        set_mock_published_connector(
            {"resource_exists": Mock(side_effect=mock_resource_exists)}
        )

        with pytest.raises(
            ValueError,
            match="Recipe file for template must be supplied in if repeating an older build without build_metadata.json",
        ):
            plan.repeat_build(
                BuildKey(product=recipe.name, build=recipe.version),
                manual_version="22v1",
            )

    def test_repeat_build_from_source_data_versions_fails_with_missing_source(self):
        import shutil
        import tempfile

        recipe = plan.recipe_from_yaml(RECIPE_PATH)

        # Create test CSV
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            test_csv = tmp_path / "source_data_versions.csv"
            shutil.copy(SOURCE_VERSIONS_PATH, test_csv)

            def mock_resource_exists(key, version, resource_path, **kw):
                return resource_path == "source_data_versions.csv"

            def mock_pull(key, version, destination_path, filepath=None, **kw):
                if filepath == "source_data_versions.csv":
                    shutil.copy(test_csv, destination_path)
                    return {"path": destination_path}
                raise FileNotFoundError()

            set_mock_published_connector(
                {
                    "resource_exists": Mock(side_effect=mock_resource_exists),
                    "pull_versioned": Mock(side_effect=mock_pull),
                }
            )

            with pytest.raises(
                Exception,
                match="Dataset found in template recipe not found in historical source data versions",
            ):
                plan.repeat_build(
                    BuildKey(product=recipe.name, build=recipe.version),
                    recipe_file=RECIPE_PATH,
                    manual_version="22v1",
                )

    def test_repeat_from_build_metadata(self):
        import shutil
        import tempfile

        # Create test file
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            test_file = tmp_path / "build_metadata.json"
            shutil.copy(BUILD_METADATA_PATH, test_file)

            def mock_resource_exists(key, version, resource_path, **kw):
                return resource_path == "build_metadata.json"

            def mock_pull(key, version, destination_path, filepath=None, **kw):
                if filepath == "build_metadata.json":
                    shutil.copy(test_file, destination_path)
                    return {"path": destination_path}
                raise FileNotFoundError()

            set_mock_published_connector(
                {
                    "resource_exists": Mock(side_effect=mock_resource_exists),
                    "pull_versioned": Mock(side_effect=mock_pull),
                }
            )

            repeat_file = plan.repeat_build(
                PublishKey(product="Tester", version="dummy")
            )
            repeat = plan.recipe_from_yaml(repeat_file)
            assert repeat.is_resolved
            assert repeat.version == "22v1"
            assert repeat.inputs.datasets[0].version == "v1"

    def test_no_record_found(self):
        # Mock connector that has no files
        set_mock_published_connector({"resource_exists": Mock(return_value=False)})

        with pytest.raises(
            Exception,
            match="Neither 'build_metadata.json' nor 'source_data_versions.csv' can be found.",
        ):
            plan.repeat_build(PublishKey(product=PRODUCT, version="version"))


@pytest.fixture(scope="function")
def reset_connectors():
    connector_registry._set_default_connectors()


class TestConnectors:
    def test_unknown_connector(self, reset_connectors):
        connector_registry.connectors._connectors = {}
        with pytest.raises(
            Exception,
            match=ConnectorRegistry.MISSING_CONN_ERROR_PREFIX,
        ):
            plan.plan_recipe(RECIPE_W_MULTIPLE_SOURCES)

    def test_plan_version_resolution(self, reset_connectors):
        MOCK_LATEST_VERSION = "123"
        CONNECTOR_NAME = "edm.custom"

        # Mock the published connector to avoid S3 access during VERSION_PREV lookup
        set_mock_published_connector({"list_versions": []})

        edm_custom_mock = MagicMock(
            get_latest_version=Mock(return_value=MOCK_LATEST_VERSION)
        )
        edm_custom_mock.conn_type = "edm.custom"

        connector_registry.connectors.register(edm_custom_mock)
        recipe = plan.plan_recipe(RECIPE_W_MULTIPLE_SOURCES)

        edm_custom_mock.get_latest_version.assert_called_once()
        resolved_custom_dataset = [
            ds for ds in recipe.inputs.datasets if ds.source == CONNECTOR_NAME
        ][0]

        assert resolved_custom_dataset.version == MOCK_LATEST_VERSION, (
            "The recipe should have the mock version applied"
        )


class TestPlanStageConfs(TestCase):
    def test_planning_conf(self):
        MY_BUILD_NOTE = "note about my build"
        os.environ["BUILD_NOTE"] = MY_BUILD_NOTE
        unplanned = plan.recipe_from_yaml(RECIPE_W_STAGES)
        assert not unplanned.is_resolved(), (
            "The recipe has unresolved vars, so it should not be considered resolved"
        )

        planned = plan.plan_recipe(RECIPE_W_STAGES)

        connector_args = planned.stage_config["builds.build"].connector_args or []

        build_note = [c.value for c in connector_args if c.name == "build_note"][0]
        assert MY_BUILD_NOTE == build_note

    def test_unresolvable_var(self):
        with self.assertRaises(Exception) as e:
            plan.recipe_from_yaml(RECIPE_W_UNRESOLVABLE_STAGES)

        assert StageConfigValue.UNRESOLVABLE_ERROR in str(e.exception), (
            "The error should mention that the stage var is unresolvable."
        )


SIMPLE_LOCK_PATH = RESOURCES_DIR / "simple.lock.yml"
SIMPLE_NO_PG_LOCK_PATH = RESOURCES_DIR / "simple_no_pg.lock.yml"


class TestWriteSourceDataVersions(TestCase):
    def test_includes_archive_date_column(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "simple.lock.yml"
            lock_path.write_text(SIMPLE_LOCK_PATH.read_text())
            plan.write_source_data_versions(lock_path)
            df = pd.read_csv(Path(tmpdir) / "source_data_versions.csv")
        assert "archive_date" in df.columns
        assert list(df["archive_date"]) == ["2024-06-01"] * len(df)

    def test_archive_date_none_writes_empty_string(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "simple_no_pg.lock.yml"
            lock_path.write_text(SIMPLE_NO_PG_LOCK_PATH.read_text())
            plan.write_source_data_versions(lock_path)
            df = pd.read_csv(
                Path(tmpdir) / "source_data_versions.csv",
                dtype=str,
                keep_default_na=False,
            )
        assert list(df["archive_date"]) == [""] * len(df)
