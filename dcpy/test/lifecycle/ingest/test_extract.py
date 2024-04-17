from pathlib import Path
from pydantic import TypeAdapter
import yaml

from dcpy.models.lifecycle.ingest import Source, LocalFileSource
from dcpy.utils import s3
from dcpy.connectors.edm import publishing
from dcpy.lifecycle.ingest import extract, configure

from . import RESOURCES


def test_download_file(create_buckets, create_temp_filesystem: Path):
    """Tests extract.download_file_from_source, depends on configure.get_filename"""
    with open(RESOURCES / "sources.yml") as f:
        sources = TypeAdapter(list[Source]).validate_python(yaml.safe_load(f))
    s3.client().put_object(
        Bucket=publishing.BUCKET,
        Key="datasets/dcp_borough_boundary/24A/dcp_borough_boundary.zip",
    )
    for source in sources:
        if isinstance(source, LocalFileSource):
            tmp_file = create_temp_filesystem / source.path
            tmp_file.touch()
            source.path = tmp_file
        test_filename = configure.get_filename(source, "test")
        extract.download_file_from_source(
            source, test_filename, "24a", dir=create_temp_filesystem
        )
        assert (create_temp_filesystem / test_filename).exists()
