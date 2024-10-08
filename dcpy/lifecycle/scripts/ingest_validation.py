from pathlib import Path
import shutil

from dcpy.utils import postgres
from dcpy.connectors.edm import recipes

from dcpy.lifecycle.ingest import run as ingest
from dcpy.data import compare


def compare_recipes_in_postgres(
    dataset: str,
    left_version: str,
    right_version: str,
    *,
    build_name: str,
    key_columns: list[str] | None = None,
    ignore_columns: list[str] | None = None,
    local_library_dir: Path = recipes.LIBRARY_DEFAULT_PATH,
    left_type: recipes.DatasetType = recipes.DatasetType.pg_dump,
    right_type: recipes.DatasetType = recipes.DatasetType.pg_dump,
):
    ignore_columns = ignore_columns or []
    ignore_columns.append("data_library_version")
    left_table = dataset + "_left"
    right_table = dataset + "_right"

    client = postgres.PostgresClient(schema=build_name, database="sandbox")
    client.drop_table(dataset)
    client.drop_table(left_table)
    client.drop_table(right_table)

    left_ds = recipes.Dataset(id=dataset, version=left_version, file_type=left_type)
    right_ds = recipes.Dataset(id=dataset, version=right_version, file_type=right_type)

    recipes.import_dataset(
        left_ds,
        client,
        import_as=left_table,
        local_library_dir=local_library_dir,
    )
    recipes.import_dataset(
        right_ds,
        client,
        import_as=right_table,
        local_library_dir=local_library_dir,
    )
    if key_columns:
        return compare.get_sql_keyed_report(
            left_table,
            right_table,
            key_columns,
            client,
            ignore_columns=ignore_columns,
        )
    else:
        return compare.get_sql_report(
            left_table,
            right_table,
            client,
            ignore_columns=ignore_columns,
        )


def run_ingest_and_library(
    dataset: str,
    ingest_parent_dir: Path = Path("."),
    library_file_type: str = "pg_dump",
):
    ingest_dir = ingest_parent_dir / dataset / "special_folder"
    ingest.run(dataset, staging_dir=ingest_dir, skip_archival=True)

    # BEWARE: once you import library, parquet file writing fails
    # Something to do with gdal's interaction with parquet file driver
    from dcpy.library.archive import Archive

    a = Archive()
    a(name=dataset, output_format=library_file_type, version="library")

    ingest_output_path = ingest_dir / f"{dataset}.parquet"
    ingest_path = (
        Path(".library") / "datasets" / dataset / "ingest" / f"{dataset}.parquet"
    )
    ingest_path.parent.mkdir(exist_ok=True, parents=True)
    shutil.copy(ingest_output_path, ingest_path)


def compare_ingest_and_library(
    dataset: str,
    key_columns: list[str] | None,
    build_name: str,
    *,
    ignore_columns: list[str] | None = None,
    library_file_type: str = "pgdump",
    ingest_parent_dir: Path = Path("."),
):
    run_ingest_and_library(
        dataset,
        ingest_parent_dir=ingest_parent_dir,
        library_file_type=library_file_type,
    )
    return compare_recipes_in_postgres(
        dataset,
        "library",
        "ingest",
        key_columns=key_columns,
        build_name=build_name,
        left_type=recipes.DatasetType.pg_dump,
        right_type=recipes.DatasetType.parquet,
        ignore_columns=ignore_columns,
    )
