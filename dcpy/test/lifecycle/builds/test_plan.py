import os
from pandas import DataFrame as df
import pytest
from unittest import TestCase
from unittest.mock import patch

from dcpy.utils import versions
from dcpy.connectors.edm import recipes
from dcpy.lifecycle.builds import plan

from dcpy.test.lifecycle.builds.conftest import REQUIRED_VERSION_ENV_VAR, RESOURCES_DIR

RECIPE_PATH = RESOURCES_DIR / "recipe.yml"
RECIPE_NO_DEFAULTS_PATH = RESOURCES_DIR / "recipe_no_defaults.yml"

MOCKED_LATEST_VERSION = "v1"


def setup():
    # TEMP_DATA_PATH.mkdir(exist_ok=True)
    os.environ[REQUIRED_VERSION_ENV_VAR] = "v123"


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
        assert (
            source_dataset_record.name == source_dataset
        ), "test setup error - check order of source datasets in recipe.yml"
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

    @patch("dcpy.connectors.edm.recipes.get_latest_version")
    def test_plan_recipe_defaults(self, get_latest_version):
        """Tests that defaults are set correctly when a recipe is planned."""
        setup()
        get_latest_version.return_value = MOCKED_LATEST_VERSION
        planned = plan.plan_recipe(RECIPE_PATH)

        had_no_version_or_type = [
            ds for ds in planned.inputs.datasets if ds.name == "has_no_version_or_type"
        ][0]

        assert (
            had_no_version_or_type.version == MOCKED_LATEST_VERSION
        ), "The missing version strategy should be applied \
            to find the latest version for this dataset."
        assert (
            had_no_version_or_type.file_type == recipes.DatasetType.csv
        ), "The datatype should default to a csv, as specified in the dataset_defaults"


@pytest.mark.usefixtures("file_setup_teardown")
@pytest.mark.usefixtures("create_buckets")
@patch("dcpy.connectors.edm.recipes.get_file_types")
class TestRecipesNoDefaults(TestCase):
    def test_plan_recipe_default_type(self, get_file_types):
        """Tests that default type is pg_dump if found when not otherwise specified."""
        setup()
        get_file_types.return_value = {
            recipes.DatasetType.pg_dump,
            recipes.DatasetType.parquet,
        }
        planned = plan.plan_recipe(RECIPE_NO_DEFAULTS_PATH)
        ds = planned.inputs.datasets[0]
        assert planned.inputs.datasets[0].file_type == recipes.DatasetType.pg_dump

    def test_serializing_and_deserializing(self, get_file_types):
        """Deserializing python models is a minefield."""
        get_file_types.return_value = {
            recipes.DatasetType.pg_dump,
            recipes.DatasetType.parquet,
        }
        lock_file = plan.plan(RECIPE_NO_DEFAULTS_PATH)
        plan.recipe_from_yaml(lock_file)


class TestRepeat(TestCase):
    def test_repeat_from_source_data_versions(self):
        recipe = plan.recipe_from_yaml(RECIPE_PATH)
        version = recipe.version
        source_data_versions = df(
            {
                "dataset": [ds.name for ds in recipe.inputs.datasets],
                "version": [
                    MOCKED_LATEST_VERSION,
                    MOCKED_LATEST_VERSION,
                    MOCKED_LATEST_VERSION,
                ],
            }
        )
        source_data_versions.set_index("dataset", inplace=True)
        repeat = plan.repeat_recipe_from_source_data_versions(
            version, source_data_versions, recipe
        )
        assert repeat.inputs.datasets[0].version == MOCKED_LATEST_VERSION
