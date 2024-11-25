from pathlib import Path
from typing import Literal

from dcpy.connectors.web import download_file
from dcpy.models.lifecycle.ingest import Source

ENDPOINT = "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/"


class BytesSource(Source):
    type: Literal["bytes"]
    dataset_id: str

    def filename(self):
        return f"{self.dataset_id}.zip"

    def download(self, version: str, output_dir: Path):
        download_file(
            f"{ENDPOINT}/{self.dataset_id}_{version}.zip", output_dir / self.filename()
        )


# Source.register(BytesSource)
