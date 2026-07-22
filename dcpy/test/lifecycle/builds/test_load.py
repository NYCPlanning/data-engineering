from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from dcpy.connectors.edm import recipes
from dcpy.lifecycle.builds import load
from dcpy.lifecycle.builds.models import (
    ImportedDataset,
    InputDataset,
    InputDatasetDestination,
    LoadResult,
)
from dcpy.test.lifecycle.builds.conftest import RESOURCES_DIR, TEMP_DATA_PATH

RECIPE_PATH = RESOURCES_DIR / "simple.lock.yml"
RECIPE_PATH_NO_PG = RESOURCES_DIR / "simple_no_pg.lock.yml"
REQUIRED_VERSION_ENV_VAR = "VERSION_PREV"

CSV = RESOURCES_DIR / "test.csv"
PARQUET = RESOURCES_DIR / "test.parquet"

_test_df = pd.DataFrame(
    [["1", "2", "test"], ["3", "4", "test"]], columns=["a", "b", "name"]
)
_test_df_numeric = _test_df.astype({"a": int, "b": int})


def _mock_preprocessor(name, df):
    df["name"] = name
    return df


def _mock_fetch_csv(ds, _):
    out_path = TEMP_DATA_PATH / f"{ds.id}.csv"
    _test_df.to_csv(out_path, index=False)
    return out_path


def _mock_fetch_parquet(ds, _=None):
    out_path = TEMP_DATA_PATH / f"{ds.id}.parquet"
    _test_df.to_parquet(out_path, index=False)
    return out_path


@pytest.mark.usefixtures("file_setup_teardown")
@pytest.mark.usefixtures("create_buckets")
class TestImportDatasets(TestCase):
    def test_import_parquet_file(self):
        # recipes.fetch_dataset = MagicMock(side_effect=_mock_fetch_parquet)

        ds = InputDataset(
            id="test",
            version="1",
            import_as="new_table_name",
            file_type=recipes.DatasetType.parquet,
            destination=InputDatasetDestination.file,
        )
        res = load.import_dataset(ds, Path("./"), None)
        assert isinstance(res.destination, Path)
        assert res.destination.exists()

    def test_no_version_fails(self):
        ds = InputDataset(id="test")
        with pytest.raises(Exception, match=load.ERROR_UNRESOLVED_DATASET_VERSION):
            load.import_dataset(ds, Path("./"), None)

    def test_unresolved_version_fails(self):
        ds = InputDataset(id="test", version="latest")
        with pytest.raises(Exception, match=load.ERROR_UNRESOLVED_DATASET_VERSION):
            load.import_dataset(ds, Path("./"), None)

    def test_unresolved_filetype(self):
        ds = InputDataset(id="test", version="1")
        with pytest.raises(Exception, match=load.ERROR_UNRESOLVED_FILE_TYPE):
            load.import_dataset(ds, Path("./"), None)


class TestGetDataset:
    load_result = LoadResult(
        name="test",
        build_name="test",
        datasets={
            "df": {
                "df_version": ImportedDataset(
                    id="df",
                    version="df",
                    file_type=recipes.DatasetType.csv,
                    destination=_test_df,
                    destination_type=InputDatasetDestination.df,
                )
            },
            "pg_dump": {
                "pg_version": ImportedDataset(
                    id="pg_dump",
                    version="pg_dump",
                    file_type=recipes.DatasetType.pg_dump,
                    destination="pg_dump",
                    destination_type=InputDatasetDestination.postgres,
                )
            },
            "csv": {
                "csv_version": ImportedDataset(
                    id="csv",
                    version="csv",
                    file_type=recipes.DatasetType.csv,
                    destination=CSV,
                    destination_type=InputDatasetDestination.file,
                )
            },
            "parquet": {
                "parquet_version": ImportedDataset(
                    id="parquet",
                    version="parquet",
                    file_type=recipes.DatasetType.parquet,
                    destination=PARQUET,
                    destination_type=InputDatasetDestination.file,
                )
            },
            "folder": {
                "folder_version": ImportedDataset(
                    id="folder",
                    version="folder",
                    file_type=recipes.DatasetType.parquet,
                    destination=RESOURCES_DIR,
                    destination_type=InputDatasetDestination.file,
                )
            },
        },
    )

    def test_df_from_df(self):
        df = load.get_imported_df(self.load_result, "df")
        assert df.equals(_test_df)

    @patch("dcpy.utils.postgres.PostgresClient")
    def test_df_from_pg(self, pg_client):
        _df = load.get_imported_df(self.load_result, "pg_dump")
        print(pg_client.return_value.read_table_df)
        pg_client.return_value.read_table_df.assert_called_with("pg_dump")

    def test_df_from_csv(self):
        df = load.get_imported_df(self.load_result, "csv")
        assert df.equals(_test_df_numeric)

    def test_df_from_parquet(self):
        df = load.get_imported_df(self.load_result, "parquet")
        assert df.equals(_test_df)

    def test_df_from_invalid_path(self):
        with pytest.raises(Exception, match="cannot be simply read by pandas"):
            load.get_imported_df(self.load_result, "folder")

    def test_df_impossible_case(self):
        with pytest.raises(Exception, match="Unknown type of imported dataset"):
            res = MagicMock()
            res.get_latest_version_str = lambda _: "version"
            res.datasets = {"df": {"version": MagicMock()}}
            load.get_imported_df(res, "df")

    def test_filepath(self):
        assert load.get_imported_filepath(self.load_result, "csv") == CSV

    @pytest.mark.parametrize("ds", ["df", "pg_dump"])
    def test_filepath_errors(self, ds):
        with pytest.raises(Exception, match="Cannot get imported file"):
            load.get_imported_filepath(self.load_result, ds)

    def test_filepath_impossible_case(self):
        with pytest.raises(Exception, match="Cannot get imported file of dataset"):
            res = MagicMock()
            res.get_latest_version_str = lambda _: "version"
            res.datasets = {"df": {"version": MagicMock()}}
            load.get_imported_filepath(res, "df")

    def test_file(self):
        with load.get_imported_file(self.load_result, "csv") as stream:
            df = pd.read_csv(stream, dtype=str)
        assert df.equals(_test_df)


class TestCachedLoad:
    """The cache is keyed by dataset id, but tables are named by `import_as`."""

    @staticmethod
    def _recipe():
        recipe = MagicMock()
        recipe.name = "Tester"
        recipe.inputs.datasets = [
            InputDataset(
                name="multi_layer_source",
                import_as="layer_b",
                version="v1",
                file_type=recipes.DatasetType.csv,
                destination=InputDatasetDestination.postgres,
            )
        ]
        return recipe

    def _load(self, table_exists: bool):
        pg_client = MagicMock()
        pg_client.table_or_view_exists.return_value = table_exists
        # source_data_versions reports the *dataset id*, so the cache looks populated
        # for every layer of a multi-layer source once any one of them is cached.
        versions = pd.DataFrame(
            [{"schema_name": "multi_layer_source", "v": "v1", "file_type": "csv"}]
        )
        with (
            patch.object(load.postgres, "PostgresClient", return_value=pg_client),
            patch.object(load, "get_source_data_versions", return_value=versions),
            patch.object(load, "setup_build_pg_schema"),
            patch.object(load.data_loader, "pull_dataset", return_value=CSV),
            patch.object(
                load,
                "import_dataset",
                return_value=ImportedDataset(
                    id="multi_layer_source",
                    version="v1",
                    file_type=recipes.DatasetType.csv,
                    destination="layer_b",
                    destination_type=InputDatasetDestination.postgres,
                ),
            ) as import_dataset,
        ):
            load.load_source_data_from_resolved_recipe(
                self._recipe(),
                cache_schema="recipe_cache",
                cached_entity_type=load.CachedEntityType.view,
                target_schema="build_schema",
                _write_metadata_file=False,
            )
        return pg_client, import_dataset

    def test_uses_cache_when_table_present(self):
        pg_client, import_dataset = self._load(table_exists=True)
        pg_client.create_view.assert_called_once_with("layer_b", "recipe_cache")
        import_dataset.assert_not_called()

    def test_falls_back_to_pull_when_table_absent(self):
        pg_client, import_dataset = self._load(table_exists=False)
        pg_client.table_or_view_exists.assert_called_once_with(
            "layer_b", schema="recipe_cache"
        )
        pg_client.create_view.assert_not_called()
        import_dataset.assert_called_once()
