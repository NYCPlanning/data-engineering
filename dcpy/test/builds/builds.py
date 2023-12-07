import os
from pandas import DataFrame as df
from pathlib import Path
import pytest
import shutil
from unittest import TestCase
from unittest.mock import MagicMock

import dcpy
from dcpy.connectors.edm import recipes
from dcpy.builds import plan

RESOURCES_DIR = Path(__file__).parent / "resources"
RECIPE_PATH = RESOURCES_DIR / "recipe.yml"
RECIPE_NO_DEFAULTS_PATH = RESOURCES_DIR / "recipe_no_defaults.yml"
RECIPE_NO_DEFAULTS_LOCK_PATH = RESOURCES_DIR / "recipe_no_defaults.lock.yml"
TEMP_DATA_PATH = RESOURCES_DIR.parent / "temp"

REQUIRED_VERSION_ENV_VAR = "VERSION_PREV"

MOCKED_LATEST_VERSION = "v1"
recipes.get_config = MagicMock(
    return_value={"dataset": {"version": MOCKED_LATEST_VERSION}}
)


@pytest.fixture
def file_setup_teardown():
    RECIPE_NO_DEFAULTS_LOCK_PATH.unlink(missing_ok=True)
    TEMP_DATA_PATH.mkdir(exist_ok=True)
    yield
    RECIPE_NO_DEFAULTS_LOCK_PATH.unlink(missing_ok=True)
    shutil.rmtree(TEMP_DATA_PATH)


def setup():
    # TEMP_DATA_PATH.mkdir(exist_ok=True)
    os.environ[REQUIRED_VERSION_ENV_VAR] = "v123"


@pytest.mark.usefixtures("file_setup_teardown")
class TestRecipesWithDefaults(TestCase):
    def test_plan_recipe_failing_env_var(self):
        """One of the datasets requires a REQUIRED_VERSION_ENV_VAR environment variable for the version.
        Plan should fail since no variable is present.
        """
        os.environ.pop(REQUIRED_VERSION_ENV_VAR)
        with self.assertRaises(Exception) as e:
            plan.plan(RECIPE_PATH)
        assert REQUIRED_VERSION_ENV_VAR in str(e.exception)

    def test_plan_recipe_defaults(self):
        """Tests that defaults are set correctly when a recipe is planned."""
        setup()
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
class TestRecipesNoDefaults(TestCase):
    def test_plan_recipe_default_type(self):
        """Tests that default type is pg_dump when not otherwise specified."""
        setup()
        planned = plan.plan_recipe(RECIPE_NO_DEFAULTS_PATH)
        ds = planned.inputs.datasets[0]
        assert ds.file_name == f"{ds.name}.sql"
        assert planned.inputs.datasets[0].file_type == recipes.DatasetType.pg_dump

    def test_serializing_and_deserializing(self):
        """Deserializing python models is a minefield."""
        lock_file = plan.plan(RECIPE_NO_DEFAULTS_PATH)
        recipes.recipe_from_yaml(lock_file)


_test_df = df([["1", "2"], ["3", "4"]], columns=["a", "b"])


def _mock_preprocessor(name, df):
    df["name"] = name
    return df


def _mock_fetch_csv(ds, _):
    out_path = TEMP_DATA_PATH / f"{ds.name}.csv"
    _test_df.to_csv(out_path, index=False)
    return out_path


def _mock_fetch_parquet(ds, _):
    out_path = TEMP_DATA_PATH / f"{ds.name}.parquet"
    _test_df.to_parquet(out_path, index=False)
    return out_path


@pytest.mark.usefixtures("file_setup_teardown")
class TestImportDatasets(TestCase):
    # TODO: move this functionality into an integration test with an actual database

    def test_import_csv(self):
        recipes.fetch_dataset = MagicMock(side_effect=_mock_fetch_csv)

        dcpy.preproc = MagicMock(side_effect=_mock_preprocessor)  # type: ignore
        pg_mock = MagicMock()
        ds = types.Dataset(
            name="test",
            version="1",
            import_as="new_table_name",
            file_type=recipes.DatasetType.csv,
            preprocessor=types.DataPreprocessor(module="dcpy", function="preproc"),
        )
        plan.import_dataset(ds, pg_mock)

        assert (
            pg_mock.insert_dataframe.called
        ), "PostgresClient.insert_dataframe should be called"

        # Verify the values that PostgresClient.insert_dataframe was called with
        (
            df_inserted_actual,
            table_insert_name_actual,
        ) = pg_mock.insert_dataframe.call_args_list[0][0]
        assert df_inserted_actual.equals(_mock_preprocessor(ds.name, _test_df))
        assert table_insert_name_actual == ds.import_as

    def test_import_parquet(self):
        recipes.fetch_dataset = MagicMock(side_effect=_mock_fetch_parquet)

        dcpy.preproc = MagicMock(side_effect=_mock_preprocessor)  # type: ignore
        pg_mock = MagicMock()
        ds = plan.Dataset(
            name="test",
            version="1",
            import_as="new_table_name",
            file_type=recipes.DatasetType.parquet,
            preprocessor=types.DataPreprocessor(module="dcpy", function="preproc"),
        )

        recipes.fetch_dataset = MagicMock(side_effect=_mock_fetch_parquet)

        plan.import_dataset(ds, pg_mock)

        (
            df_inserted_actual,
            table_insert_name_actual,
        ) = pg_mock.insert_dataframe.call_args_list[0][0]
        assert df_inserted_actual.equals(_mock_preprocessor(ds.name, _test_df))
        assert table_insert_name_actual == ds.import_as
