import pytest
from pathlib import Path

from dcpy.lifecycle.package import oti_xlsx


@pytest.fixture
def package_path(resources_path: Path):
    return resources_path / "product_metadata" / "assembled_package_and_metadata"


def test_generate_xslx(
    package_path,
    tmp_path,
):
    """A very basic test just to exercise the reading/writing xlsx files."""
    output_path = tmp_path / "my_data_dictionary.xlsx"

    assert not output_path.exists()
    oti_xlsx.write_oti_xlsx(
        metadata_path=package_path / "metadata.yml",
        output_path=output_path,
    )
    assert output_path.exists()
