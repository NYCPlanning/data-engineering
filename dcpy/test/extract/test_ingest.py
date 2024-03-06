from pathlib import Path
import yaml

from dcpy.utils import s3
from dcpy.connectors.edm import publishing
from dcpy.extract import metadata, ingest

RESOURCES = Path(__file__).parent / "resources"


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
        template = metadata.Template(
            name="test",
            acl="public-read",
            source=source,
            transform_to_parquet_metadata=None,
        )
        file = ingest.download_file_from_source(template, "24a")
        assert file.exists()
