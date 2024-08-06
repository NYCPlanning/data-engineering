from pandas import DataFrame as df
import pytest
from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import patch

from dcpy.models.lifecycle.builds import InputDataset, DataPreprocessor
from dcpy.connectors.edm import recipes
from dcpy.lifecycle.builds import load

from dcpy.test.lifecycle.builds.conftest import TEMP_DATA_PATH


REQUIRED_VERSION_ENV_VAR = "VERSION_PREV"

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
@pytest.mark.usefixtures("create_buckets")
class TestImportDatasets(TestCase):
    # TODO: move this functionality into an integration test with an actual database

    @patch("dcpy.preproc", side_effect=_mock_preprocessor, create=True)
    def test_import_csv(self, _patched):
        recipes.fetch_dataset = MagicMock(side_effect=_mock_fetch_csv)

        pg_mock = MagicMock()
        ds = InputDataset(
            name="test",
            version="1",
            import_as="new_table_name",
            file_type=recipes.DatasetType.csv,
            preprocessor=DataPreprocessor(module="dcpy", function="preproc"),
        )
        load.import_dataset(ds, pg_mock)

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

    @patch("dcpy.preproc", side_effect=_mock_preprocessor, create=True)
    def test_import_parquet(self, _patched):
        recipes.fetch_dataset = MagicMock(side_effect=_mock_fetch_parquet)

        pg_mock = MagicMock()
        ds = InputDataset(
            name="test",
            version="1",
            import_as="new_table_name",
            file_type=recipes.DatasetType.parquet,
            preprocessor=DataPreprocessor(module="dcpy", function="preproc"),
        )

        load.import_dataset(ds, pg_mock)

        (
            df_inserted_actual,
            table_insert_name_actual,
        ) = pg_mock.insert_dataframe.call_args_list[0][0]
        assert df_inserted_actual.equals(_mock_preprocessor(ds.name, _test_df))
        assert table_insert_name_actual == ds.import_as

    def test_no_version_fails(self):
        ds = InputDataset(name="test")
        with pytest.raises(Exception, match="Cannot import"):
            load.import_dataset(ds, MagicMock)

    def test_unresolved_version_fails(self):
        ds = InputDataset(name="test", version="latest")
        with pytest.raises(Exception, match="Cannot import"):
            load.import_dataset(ds, MagicMock)
