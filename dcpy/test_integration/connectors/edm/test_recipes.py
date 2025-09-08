import pytest
import time
from cloudpathlib import AnyPath

from dcpy.connectors.edm.recipes import RecipesRepo
from dcpy.connectors.hybrid_pathed_storage import (
    HybridPathedStorage,
)


@pytest.fixture
def az_recipes_repo(tmp_path):
    return RecipesRepo(
        local_recipes_path=tmp_path,
        remote_repo=HybridPathedStorage.Factory.azure(az_container_name="test"),
    )


def _make_fake_datasets():
    datasets = ["dob_cofos", "dcp_pluto", "ed_asdf"]
    versions = ["v1", "v2"]
    file_types = ["parquet", "pg_dump", "csv"]
    combos = []
    for ds in datasets:
        for v in versions:
            for ft in file_types:
                combos.append((ds, v, ft))
    return combos


def create_files(p: AnyPath):
    # Create a folder with the epoch time of the test run
    epoch = str(int(time.time()))
    # Use AnyPath to generalize between local and cloud paths
    base_dir = p / "tmp" / epoch
    base_dir.mkdir(parents=True, exist_ok=True)
    created_files = []
    for ds, v, ft in _make_fake_datasets():
        file_path = base_dir / "datasets" / ds / v / f"{ds}.{ft}"
        AnyPath(file_path).touch()
        created_files.append(file_path)
    return base_dir


def remove_files(base_dir, created_files):
    # TODO fix this up
    for f in created_files:
        AnyPath(f).unlink()
    AnyPath(base_dir).rmdir()


@pytest.fixture(scope="session")
def setup_local_files(tmp_path):
    recipes_dir = create_files(AnyPath(tmp_path))
    yield recipes_dir
    remove_files(recipes_dir)


# @pytest.fixture(scope="session")
# def setup_azure_files(tmp_path):
#     "TODO"


# @pytest.fixture(scope="session")
# def setup_s3_files(tmp_path):
#     "TODO"


@pytest.mark.parametrize("ds,v,ft", _make_fake_datasets())
def test_local_exists(setup_fixture, ds, v, ft):
    base_dir = setup_fixture

    connector = RecipesConnector(
        storage_backend=StorageBackend.LOCAL,
        local_dir=base_dir,
    )

    dataset_obj = Dataset(id=ds, version=v, file_type=ft)
    assert connector.exists(dataset_obj)
