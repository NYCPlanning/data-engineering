from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

import pandas as pd

from dcpy.lifecycle import data_loader


def _write_parquet(dir: str, n_rows: int) -> Path:
    filepath = Path(dir) / "test.parquet"
    pd.DataFrame({"my col": range(n_rows)}).to_parquet(filepath, index=False)
    return filepath


def test_load_parquet_chunked_replaces_then_appends(monkeypatch):
    # 5 rows at batch size 2 -> 3 batches: replace, append, append
    monkeypatch.setattr(data_loader, "PARQUET_LOAD_BATCH_SIZE", 2)
    pg_client = MagicMock()

    with TemporaryDirectory() as dir:
        filepath = _write_parquet(dir, 5)
        data_loader._load_parquet_chunked(filepath, "my_table", pg_client)

    if_exists_seq = [
        call.kwargs.get("if_exists", call.args[2] if len(call.args) > 2 else None)
        for call in pg_client.insert_dataframe.call_args_list
    ]
    assert if_exists_seq == ["replace", "append", "append"]

    # columns are sanitized before insert, and the pk is added once at the end
    first_df = pg_client.insert_dataframe.call_args_list[0].args[0]
    assert list(first_df.columns) == ["my_col"]
    pg_client.add_pk.assert_called_once_with("my_table", "ogc_fid")


def test_load_parquet_chunked_skips_pk_when_not_requested(monkeypatch):
    monkeypatch.setattr(data_loader, "PARQUET_LOAD_BATCH_SIZE", 2)
    pg_client = MagicMock()

    with TemporaryDirectory() as dir:
        filepath = _write_parquet(dir, 3)
        data_loader._load_parquet_chunked(
            filepath, "my_table", pg_client, include_ogc_fid_col=False
        )

    pg_client.add_pk.assert_not_called()


def test_load_df_drops_loader_owned_columns():
    pg_client = MagicMock()
    df = pd.DataFrame(
        {
            "ogc_fid": [1, 2],
            "version": ["25Q4", "26Q2"],
            "data_library_version": ["20240101", "20240101"],
        }
    )

    data_loader._load_df(df, "my_table", pg_client)

    inserted = pg_client.insert_dataframe.call_args.args[0]
    assert list(inserted.columns) == ["version"]
    pg_client.add_pk.assert_called_once_with("my_table", "ogc_fid")
