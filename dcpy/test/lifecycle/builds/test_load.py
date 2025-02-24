import pandas as pd
from pathlib import Path
import pytest
from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import patch

from dcpy.models.lifecycle.builds import (
    InputDataset,
    InputDatasetDestination,
    ImportedDataset,
    LoadResult,
)
from dcpy.connectors.edm import recipes
from dcpy.lifecycle.builds import load

from dcpy.test.lifecycle.builds.conftest import TEMP_DATA_PATH, RESOURCES_DIR

RECIPE_PATH = RESOURCES_DIR / "simple.lock.yml"
RECIPE_PATH_NO_PG = RESOURCES_DIR / "simple_no_pg.lock.yml"
REQUIRED_VERSION_ENV_VAR = "VERSION_PREV"

CSV = RESOURCES_DIR / "test.csv"
PARQUET = RESOURCES_DIR / "test.parquet"

_test_df = pd.DataFrame(
    [["1", "2", "test"], ["3", "4", "test"]], columns=["a", "b", "name"]
)


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
    @patch("dcpy.connectors.edm.recipes.read_df", return_value=_test_df)
    def test_import_parquet_df(self, read_df):
        ds = InputDataset(
            id="test",
            version="1",
            import_as="new_table_name",
            file_type=recipes.DatasetType.parquet,
            destination=InputDatasetDestination.df,
        )
        res = load.import_dataset(ds, Path("./"), None)
        assert isinstance(res.destination, pd.DataFrame)
        assert _test_df.equals(res.destination)

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
            "df": ImportedDataset(
                id="df",
                version="df",
                file_type=recipes.DatasetType.csv,
                destination=_test_df,
            ),
            "pg_dump": ImportedDataset(
                id="pg_dump",
                version="pg_dump",
                file_type=recipes.DatasetType.pg_dump,
                destination="pg_dump",
            ),
            "csv": ImportedDataset(
                id="csv",
                version="csv",
                file_type=recipes.DatasetType.csv,
                destination=CSV,
            ),
            "parquet": ImportedDataset(
                id="parquet",
                version="parquet",
                file_type=recipes.DatasetType.parquet,
                destination=PARQUET,
            ),
            "folder": ImportedDataset(
                id="folder",
                version="folder",
                file_type=recipes.DatasetType.parquet,
                destination=RESOURCES_DIR,
            ),
        },
    )

    def test_df_from_df(self):
        df = load.get_imported_df(self.load_result, "df")
        assert df.equals(_test_df)

    @patch("dcpy.utils.postgres.PostgresClient")
    def test_df_from_pg(self, pg_client):
        _df = load.get_imported_df(self.load_result, "pg_dump")
        pg_client.return_value.read_table_df.assert_called_with("pg_dump")

    def test_df_from_csv(self):
        df = load.get_imported_df(self.load_result, "csv")
        assert df.equals(_test_df)

    def test_df_from_parquet(self):
        df = load.get_imported_df(self.load_result, "parquet")
        assert df.equals(_test_df)

    def test_df_from_invalid_path(self):
        with pytest.raises(Exception, match="cannot be simply read by pandas"):
            load.get_imported_df(self.load_result, "folder")

    def test_df_impossible_case(self):
        with pytest.raises(Exception, match="Unknown type of imported dataset"):
            res = MagicMock()
            res.datasets = {"df": MagicMock()}
            load.get_imported_df(res, "df")

    def test_filepath(self):
        assert load.get_imported_filepath(self.load_result, "csv") == CSV

    @pytest.mark.parametrize("ds", ["df", "pg_dump"])
    def test_filepath_errors(self, ds):
        with pytest.raises(Exception, match="Cannot get imported file"):
            load.get_imported_filepath(self.load_result, ds)

    def test_filepath_impossible_case(self):
        with pytest.raises(Exception, match="Cannot get imported file"):
            res = MagicMock()
            res.datasets = {"df": MagicMock()}
            load.get_imported_filepath(res, "df")

    def test_file(self):
        with load.get_imported_file(self.load_result, "csv") as stream:
            df = pd.read_csv(stream, dtype=str)
        assert df.equals(_test_df)
