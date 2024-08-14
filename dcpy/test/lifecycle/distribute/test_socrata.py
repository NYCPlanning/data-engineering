from pathlib import Path
import pytest
from unittest.mock import patch, call

import dcpy.models.product.dataset.metadata_v2 as md
import dcpy.lifecycle.distribute.socrata as soc_dist


@pytest.fixture
def package_path(resources_path: Path):
    return resources_path / "product_metadata" / "assembled_package_and_metadata"


@pytest.fixture
def metadata(package_path: Path):
    return md.Metadata.from_path(package_path / "metadata.yml")


@patch("dcpy.connectors.socrata.publish.push_dataset")
def test_dist_local_product_all_socrata(pub_mock, package_path, metadata):
    """Test that the Socrata push connector is invoked on each dataset destination in
    the metadata file.
    """
    soc_dist.dist_from_local_product_all_socrata(package_path.parent)

    actual_destinations = [
        c.kwargs["dataset_destination_id"] for c in pub_mock.mock_calls
    ]
    assert len(actual_destinations) == len(pub_mock.mock_calls)

    expected_destinations = [d.id for d in metadata.destinations if d.type == "socrata"]
    assert expected_destinations == actual_destinations
