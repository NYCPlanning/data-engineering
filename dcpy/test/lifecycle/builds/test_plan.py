import os
import pytest
from unittest import TestCase
from unittest.mock import patch

from dcpy.connectors.edm import recipes
from dcpy.lifecycle.builds import plan

from dcpy.test.lifecycle.builds.conftest import REQUIRED_VERSION_ENV_VAR, RESOURCES_DIR

RECIPE_PATH = RESOURCES_DIR / "recipe.yml"
RECIPE_NO_DEFAULTS_PATH = RESOURCES_DIR / "recipe_no_defaults.yml"

MOCKED_LATEST_VERSION = "v1"


def setup():
    # TEMP_DATA_PATH.mkdir(exist_ok=True)
    os.environ[REQUIRED_VERSION_ENV_VAR] = "v123"


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
