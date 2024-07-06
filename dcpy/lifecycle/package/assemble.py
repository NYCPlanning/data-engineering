from pathlib import Path
import typer
import tempfile
import urllib.request
import zipfile
import shutil

import dcpy.models.product.dataset.metadata as models
from dcpy.utils.logging import logger
from dcpy.lifecycle import WORKING_DIRECTORIES
from dcpy.connectors.edm import packaging

ASSEMBLY_DIR = WORKING_DIRECTORIES.packaging / ".assembly"


def from_bytes(
    product_metadata: models.Metadata, product: str, version: str, dataset: str
):
    out_dir = ASSEMBLY_DIR / product / version / dataset
    out_dir.mkdir(exist_ok=True, parents=True)

    product_metadata.write_source_metadata_to_file(Path(out_dir / "metadata.yml"))

    for bytes_dest in [d for d in product_metadata.destinations if d.type == "bytes"]:
        package_files_by_id = product_metadata.package.files_by_id()

        for dest_file in bytes_dest.files:
            f = package_files_by_id[dest_file.id]
            file_path = Path(out_dir) / f.filename
            logger.info(f"downloading {f.filename} to {file_path}")
            urllib.request.urlretrieve(dest_file.url, file_path)

            if type(f) == models.ZipFile:
                logger.info(f"Unzipping: {f.id} to retrieve contents")
                with tempfile.TemporaryDirectory() as temp_unpacked_dir:
                    shutil.unpack_archive(file_path, temp_unpacked_dir)
                    for contained_file in f.contains:
                        cf = package_files_by_id[contained_file.id]
                        logger.info(f"Extracting zipped file: {cf.id}")
                        shutil.copy2(
                            Path(temp_unpacked_dir) / contained_file.filename,  # type: ignore
                            out_dir / cf.filename,
                        )


app = typer.Typer()


@app.command("from_bytes")
def _from_bytes_cli(metadata_path: Path, product: str, version: str, dataset: str):
    md = models.Metadata.from_path(
        metadata_path,
        template_vars={"version": version},
    )
    from_bytes(md, product, version, dataset)
