import os
import pandas as pd
import pytest
from unittest import TestCase
from unittest.mock import patch, MagicMock, Mock

from dcpy.connectors.registry import ConnectorRegistry
from dcpy.models.lifecycle.builds import InputDataset, StageConfigValue
from dcpy.utils import versions
from dcpy.connectors.edm import recipes, publishing
from dcpy.lifecycle.builds import plan
from dcpy.lifecycle import connector_registry

from dcpy.test.lifecycle.builds.conftest import REQUIRED_VERSION_ENV_VAR, RESOURCES_DIR

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

    def test_first_of_month(self):
        self.recipe.version_strategy = versions.SimpleVersionStrategy.first_of_month
        assert (
            plan.resolve_version(self.recipe)
            == versions.generate_first_of_month().label
        )

    @patch("dcpy.connectors.edm.publishing.get_latest_version")
    def test_simple_bump_latest_release(self, get_latest):
        self.recipe.version_strategy = (
            versions.SimpleVersionStrategy.bump_latest_release
        )
        get_latest.return_value = "24v1"
        assert plan.resolve_version(self.recipe) == "24v2"

    @patch("dcpy.connectors.edm.publishing.get_latest_version")
    def test_bump_latest_release(self, get_latest):
        self.recipe.version_strategy = versions.BumpLatestRelease(bump_latest_release=2)
        get_latest.return_value = "24v1"
        assert plan.resolve_version(self.recipe) == "24v3"

    @patch("dcpy.connectors.edm.recipes.get_latest_version")
    def test_pin_to_source_dataset(self, get_latest):
        version = "20240101"
        get_latest.return_value = version
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
@patch("dcpy.connectors.edm.recipes.get_latest_version")
class TestRecipesWithDefaults(TestCase):
    def test_plan_recipe_failing_env_var(self, get_latest_version):
        """One of the datasets requires a REQUIRED_VERSION_ENV_VAR environment variable for the version.
        Plan should fail since no variable is present.
        """
        os.environ.pop(REQUIRED_VERSION_ENV_VAR)
        with self.assertRaises(Exception) as e:
            plan.plan(RECIPE_PATH)
            assert REQUIRED_VERSION_ENV_VAR in str(e.exception)

    def test_provide_manual_version(self, get_latest_version):
        add_required_version_var_to_env()
        version = "test_version"
        planned = plan.plan_recipe(RECIPE_PATH, version=version)
        assert planned.version == version
        assert planned.is_resolved, "Dataset is not resolved"

    def test_plan_recipe_defaults(self, get_latest_version):
        """Tests that defaults are set correctly when a recipe is planned."""
        add_required_version_var_to_env()
        get_latest_version.return_value = MOCKED_LATEST_VERSION
        planned = plan.plan_recipe(RECIPE_PATH)

        missing_source = [ds for ds in planned.inputs.datasets if not ds.source]
        assert not missing_source

        had_no_version_or_type = [
            ds for ds in planned.inputs.datasets if ds.id == "has_no_version_or_type"
        ][0]

        assert had_no_version_or_type.version == MOCKED_LATEST_VERSION, (
            "The missing version strategy should be applied \
            to find the latest version for this dataset."
        )
        assert had_no_version_or_type.file_type == recipes.DatasetType.csv, (
            "The datatype should default to a csv, as specified in the dataset_defaults"
        )
        assert planned.is_resolved, "Dataset is not resolved"


@pytest.mark.usefixtures("create_buckets")
class TestRecipesWithNoVersion(TestCase):
    def test_no_provided_recipe_fails(self):
        with pytest.raises(Exception, match="No version or version_strategy provided"):
            plan.plan_recipe(RECIPE_NO_VERSION_PATH)

    @patch("dcpy.connectors.edm.recipes.get_latest_version")
    def test_provide_manual_version(self, get_latest_version):
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
            recipes.DatasetType.pg_dump,
            recipes.DatasetType.parquet,
        }
        planned = plan.plan_recipe(RECIPE_NO_DEFAULTS_PATH)
        assert planned.inputs.datasets[0].file_type == recipes.DatasetType.pg_dump
        assert planned.is_resolved, "Dataset is not resolved"

    def test_serializing_and_deserializing(self, get_file_types):
        """Deserializing python models is a minefield."""
        get_file_types.return_value = {
            recipes.DatasetType.pg_dump,
            recipes.DatasetType.parquet,
        }
        lock_file = plan.plan(RECIPE_NO_DEFAULTS_PATH)
        plan.recipe_from_yaml(lock_file)


@patch("dcpy.connectors.edm.recipes.get_latest_version")
class TestRecipeVars(TestCase):
    def test_version_type_var_is_absent(self, get_latest_version):
        """Ensures VERSION_TYPE is absent in recipe 'vars' attribute when version_type is None."""
        add_required_version_var_to_env()
        version = "test_version"
        planned = plan.plan_recipe(RECIPE_PATH, version=version)
        assert planned.version_type is None  # sanity check
        assert "VERSION_TYPE" not in planned.vars

    def test_version_type_is_present(self, get_latest_version):
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
    @patch("dcpy.connectors.edm.publishing.get_source_data_versions")
    @patch(
        "dcpy.connectors.edm.publishing.file_exists",
        side_effect=source_versions_csv_exists,
    )
    def test_repeat_published_version_from_source_data_versions(
        self, file_exists, source_data
    ):
        version = "21v1"
        recipe = plan.recipe_from_yaml(RECIPE_NO_DEFAULTS_PATH)
        source_data.return_value = pd.read_csv(
            RESOURCES_DIR / "source_data_versions.csv"
        ).set_index("dataset")
        repeat_file = plan.repeat_build(
            publishing.PublishKey(product=recipe.name, version=version),
            recipe_file=RECIPE_NO_DEFAULTS_PATH,
        )
        repeat = plan.recipe_from_yaml(repeat_file)
        assert repeat.is_resolved
        assert repeat.version == version
        assert repeat.inputs.datasets[0].version == "v1"

    @patch("dcpy.connectors.edm.publishing.get_source_data_versions")
    @patch(
        "dcpy.connectors.edm.publishing.file_exists",
        side_effect=source_versions_csv_exists,
    )
    def test_repeat_build_from_source_data_versions(self, file_exists, source_data):
        recipe = plan.recipe_from_yaml(RECIPE_NO_DEFAULTS_PATH)
        source_data.return_value = pd.read_csv(
            RESOURCES_DIR / "source_data_versions.csv"
        ).set_index("dataset")
        repeat_file = plan.repeat_build(
            publishing.BuildKey(product=recipe.name, build=recipe.version),
            recipe_file=RECIPE_NO_DEFAULTS_PATH,
            manual_version="21v1",
        )
        repeat = plan.recipe_from_yaml(repeat_file)
        assert repeat.is_resolved
        assert repeat.version == "21v1"
        assert repeat.inputs.datasets[0].version == "v1"

    @patch(
        "dcpy.connectors.edm.publishing.file_exists",
        side_effect=source_versions_csv_exists,
    )
    def test_repeat_build_from_source_data_versions_fails_without_version(
        self, file_exists
    ):
        recipe = plan.recipe_from_yaml(RECIPE_NO_DEFAULTS_PATH)
        with pytest.raises(
            ValueError,
            match="Version must be supplied manually if repeating an older build without build_metadata.json",
        ):
            plan.repeat_build(
                publishing.BuildKey(product=recipe.name, build=recipe.version),
                recipe_file=RECIPE_NO_DEFAULTS_PATH,
            )

    @patch(
        "dcpy.connectors.edm.publishing.file_exists",
        side_effect=source_versions_csv_exists,
    )
    def test_repeat_build_from_source_data_versions_fails_without_recipe_path(
        self, file_exists
    ):
        recipe = plan.recipe_from_yaml(RECIPE_NO_DEFAULTS_PATH)
        with pytest.raises(
            ValueError,
            match="Recipe file for template must be supplied in if repeating an older build without build_metadata.json",
        ):
            plan.repeat_build(
                publishing.BuildKey(product=recipe.name, build=recipe.version),
                manual_version="22v1",
            )

    @patch("dcpy.connectors.edm.publishing.get_source_data_versions")
    @patch(
        "dcpy.connectors.edm.publishing.file_exists",
        side_effect=source_versions_csv_exists,
    )
    def test_repeat_build_from_source_data_versions_fails_with_missing_source(
        self, file_exists, source_data
    ):
        recipe = plan.recipe_from_yaml(RECIPE_PATH)
        source_data.return_value = pd.read_csv(
            RESOURCES_DIR / "source_data_versions.csv"
        ).set_index("dataset")
        with pytest.raises(
            Exception,
            match="Dataset found in template recipe not found in historical source data versions",
        ):
            plan.repeat_build(
                publishing.BuildKey(product=recipe.name, build=recipe.version),
                recipe_file=RECIPE_PATH,
                manual_version="22v1",
            )

    @patch("dcpy.connectors.edm.publishing.get_file")
    @patch(
        "dcpy.connectors.edm.publishing.file_exists", side_effect=build_metadata_exists
    )
    def test_repeat_from_build_metadata(self, build_metadata, get_file):
        get_file.return_value = open(BUILD_METADATA_PATH)
        repeat_file = plan.repeat_build(
            publishing.PublishKey(product="Tester", version="dummy")
        )
        repeat = plan.recipe_from_yaml(repeat_file)
        assert repeat.is_resolved
        assert repeat.version == "22v1"
        assert repeat.inputs.datasets[0].version == "v1"

    def test_no_record_found(self):
        with pytest.raises(
            Exception,
            match="Neither 'build_metadata.json' nor 'source_data_versions.csv' can be found.",
        ):
            plan.repeat_build(publishing.PublishKey(product=PRODUCT, version="version"))


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
