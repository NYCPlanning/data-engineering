from pathlib import Path
import yaml

from dcpy.models import file
from dcpy.utils import s3
from dcpy.connectors.edm import publishing
from dcpy.lifecycle.ingest import (
    configure,
    extract,
)
from . import RESOURCES


def test_download_file(create_buckets, create_temp_filesystem: Path):
    with open(RESOURCES / "sources.yml") as f:
        sources = yaml.safe_load(f)
    s3.client().put_object(
        Bucket=publishing.BUCKET,
        Key="datasets/dcp_borough_boundary/24A/dcp_borough_boundary.zip",
    )
    for source in sources:
        if source["type"] == "local_file":
            tmp_file = create_temp_filesystem / "tmp.txt"
            tmp_file.touch()
            source["path"] = tmp_file
        template = configure.Template(
            name="test",
            acl="public-read",
            source=source,
            file_format=file.Csv(format="csv"),  # easiest to mock
        )

        local_file = extract.download_file_from_source(
            template, "24a", dir=create_temp_filesystem
        )
        assert local_file.exists()
