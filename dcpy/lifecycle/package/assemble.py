from pathlib import Path
import typer
import tempfile
import urllib.request
import zipfile
import shutil

import dcpy.models.product.dataset.metadata as models
from dcpy.utils.logging import logger
from dcpy.lifecycle import WORKING_DIRECTORIES

ASSEMBLY_DIR = WORKING_DIRECTORIES.packaging / ".assembly"


def from_bytes(
    product_metadata: models.Metadata, product: str, version: str, dataset: str
):
    out_dir = ASSEMBLY_DIR / product / version / dataset
    out_dir.mkdir(exist_ok=True, parents=True)

    for bytes_dest in [d for d in product_metadata.destinations if d.type == "bytes"]:
        package_files = product_metadata.package._files_by_name()

        for dest_file in bytes_dest.files:
            f = package_files[dest_file.name]
            file_name: str = f.filename if issubclass(type(f), models.File) else f  # type: ignore
            file_path = Path(out_dir) / file_name
            logger.info(f"downloading {file_name} to {file_path}")
            urllib.request.urlretrieve(dest_file.url, file_path)

            if type(f) == models.ZipFile:
                with tempfile.TemporaryDirectory() as temp_unpacked_dir:
                    shutil.unpack_archive(file_path, temp_unpacked_dir)
                    for contained_file in f.contains:
                        cf = package_files[contained_file.name]
                        cf_file_name: str = cf.filename if issubclass(type(cf), models.File) else cf  # type: ignore
                        shutil.copy2(
                            Path(temp_unpacked_dir) / contained_file.filename,  # type: ignore
                            out_dir / cf_file_name,
                        )
