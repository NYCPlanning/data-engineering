import pandas as pd
import pytest

from dcpy.models.lifecycle.builds import ExportFormat
from dcpy.utils import postgres
from dcpy.lifecycle.builds import build


SAMPLE_TABLE = "test_export_data"


@pytest.fixture
def client_with_sample_data(clean_build_engine: postgres.PostgresClient):
    """Create sample test data in the database."""

    clean_build_engine.create_schema()
    test_data = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "value": [10.5, 20.3, 30.1, 40.9, 50.2],
            "category": ["A", "B", "A", "C", "B"],
        }
    )

    clean_build_engine.insert_dataframe(test_data, table_name=SAMPLE_TABLE)
    return clean_build_engine


def test_export_to_csv(client_with_sample_data, tmp_path):
    """Test CSV export with headers."""
    output_file = tmp_path / "test_export.csv"

    build.export_dataset_from_postgres(
        table_name=SAMPLE_TABLE,
        file_path=output_file,
        format=ExportFormat.csv,
        pg_client=client_with_sample_data,
    )

    assert output_file.exists(), "CSV export file should be created"

    # Verify contents
    exported_df = pd.read_csv(output_file)
    assert len(exported_df) == 5, "Should have 5 rows of data"
    assert list(exported_df.columns) == ["id", "name", "value", "category"], (
        "Should have correct column headers"
    )
    assert exported_df["name"].tolist() == ["Alice", "Bob", "Charlie", "Diana", "Eve"]


def test_export_to_dat(client_with_sample_data, tmp_path):
    """Test DAT export without headers and with CRLF line endings."""
    output_file = tmp_path / "test_export.dat"

    build.export_dataset_from_postgres(
        table_name=SAMPLE_TABLE,
        file_path=output_file,
        format=ExportFormat.dat,
        pg_client=client_with_sample_data,
    )

    assert output_file.exists(), "DAT export file should be created"

    # Read the file in binary mode to check line endings
    with open(output_file, "rb") as f:
        content = f.read()
    assert b"\r\n" in content, "DAT export should have CRLF line endings"

    # Actual testing of string content
    with open(output_file, "r") as f:
        lines = f.readlines()

    first_line = lines[0]
    assert first_line != "id,name,value,category", "DAT export should not have headers"
    assert first_line.startswith("1,"), "First line should start with id value"
    assert len(lines) == 5, "Should have exactly 5 rows without header"


def test_export_unsupported_format(client_with_sample_data, tmp_path):
    """Test that unsupported formats raise NotImplementedError."""
    output_file = tmp_path / "test_export.parquet"

    with pytest.raises(NotImplementedError, match="Export of dataset format"):
        build.export_dataset_from_postgres(
            table_name=SAMPLE_TABLE,
            file_path=output_file,
            format=ExportFormat.parquet,
            pg_client=client_with_sample_data,
        )


def test_export(client_with_sample_data, tmp_path):
    """Test export function with datasets in multiple formats (CSV and DAT)."""
    output_folder = tmp_path / "output"

    recipe_content = f"""
name: test_recipe
product: test
inputs: {{}}
exports:
  output_folder: {output_folder}
  datasets:
    - name: {SAMPLE_TABLE}
      format: csv
    - name: {SAMPLE_TABLE}
      filename: data.dat
      format: dat
  zip_name: output
"""

    recipe_path = tmp_path / "recipe.lock.yml"
    recipe_path.write_text(recipe_content)

    build.export(recipe_path, client_with_sample_data)

    assert (output_folder / f"{SAMPLE_TABLE}.csv").exists(), "CSV export should exist"
    assert (output_folder / "data.dat").exists(), "DAT export should exist"

    assert (output_folder / "output.zip").exists(), "zipped export should exist"


def test_export_default_path(client_with_sample_data, tmp_path):
    """Test export function with datasets in multiple formats (CSV and DAT)."""

    recipe_content = f"""
name: test_recipe
product: test
inputs: {{}}
exports:
  datasets:
    - name: {SAMPLE_TABLE}
      format: csv
"""

    recipe_path = tmp_path / "recipe.lock.yml"
    recipe_path.write_text(recipe_content)

    output_folder = build.export(recipe_path, client_with_sample_data)

    assert output_folder, "Exports should be run and output folder Path object returned"

    assert (output_folder / f"{SAMPLE_TABLE}.csv").exists(), "CSV export should exist"
