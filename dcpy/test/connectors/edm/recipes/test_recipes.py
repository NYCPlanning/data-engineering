import os
from pathlib import Path
import pytest
from unittest import TestCase
from unittest.mock import MagicMock
from dcpy.connectors.edm import recipes

RESOURCES_DIR = Path(__file__).parent / "resources"
RECIPE_PATH = RESOURCES_DIR / "recipe.yml"
RECIPE_NO_DEFAULTS_PATH = RESOURCES_DIR / "recipe_no_defaults.yml"
RECIPE_NO_DEFAULTS_LOCK_PATH = RESOURCES_DIR / "recipe_no_defaults.lock.yml"

REQUIRED_VERSION_ENV_VAR = "VERSION_PREV"

MOCKED_LATEST_VERSION = "v1"
recipes.get_config = MagicMock(
    return_value={"dataset": {"version": MOCKED_LATEST_VERSION}}
)


@pytest.fixture
def file_cleanup():
    RECIPE_NO_DEFAULTS_LOCK_PATH.unlink(missing_ok=True)
    yield
    RECIPE_NO_DEFAULTS_LOCK_PATH.unlink(missing_ok=True)


@pytest.mark.usefixtures("file_cleanup")
class TestRecipesWithDefaults(TestCase):
    def test_plan_recipe_failing_env_var(self):
        """One of the datasets requires a REQUIRED_VERSION_ENV_VAR environment variable for the version.
        Plan should fail since no variable is present.
        """
        os.environ.pop(REQUIRED_VERSION_ENV_VAR)
        with self.assertRaises(Exception) as e:
            recipes.plan(RECIPE_PATH)
        assert REQUIRED_VERSION_ENV_VAR in str(e.exception)

    def test_plan_recipe_defaults(self):
        """Tests that defaults are set correctly when a recipe is planned."""
        DS_VERSION = "v123"
        os.environ[REQUIRED_VERSION_ENV_VAR] = DS_VERSION

        planned = recipes.plan_recipe(RECIPE_PATH)

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


@pytest.mark.usefixtures("file_cleanup")
class TestRecipesNoDefaults(TestCase):
    def test_plan_recipe_default_type(self):
        """Tests that default type is pg_dump when not otherwise specified."""
        planned = recipes.plan_recipe(RECIPE_NO_DEFAULTS_PATH)
        ds = planned.inputs.datasets[0]
        assert ds.file_name == f"{ds.name}.sql"
        assert planned.inputs.datasets[0].file_type == recipes.DatasetType.pg_dump

    def test_serializing_and_deserializing(self):
        """Deserializing python models is a minefield."""
        lock_file = recipes.plan(RECIPE_NO_DEFAULTS_PATH)
        recipes.recipe_from_yaml(lock_file)
