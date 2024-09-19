from pathlib import Path
import typer
import urllib.request

import dcpy.models.product.dataset.metadata_v2 as md
from dcpy.utils.logging import logger
from dcpy.lifecycle.package import assemble

BYTES_DEST_TYPE = "bytes"
NON_BYTES_DEST_ERROR = "Cannot distribute to non-bytes destination types"


def _get_file_url_mappings_by_id(
    product_metadata: md.Metadata, destination_id: str
) -> dict[str, dict]:
    """Find the destination urls for all destination files and zips in a package."""
    ids_to_paths_and_dests = {}

    for df in product_metadata.get_destination(destination_id).files:
        # The destination file could be either a `file` or a `package` zip
        files = [f for f in product_metadata.files if f.file.id == df.id]
        packages = [p for p in product_metadata.assembly if p.id == df.id]
        if files:
            f = files[0]
            folder = "attachments" if f.file.is_metadata else "dataset_files"
            ids_to_paths_and_dests[df.id] = {
                "path": f"{folder}/{f.file.filename}",
                "url": df.custom["url"],
            }
        elif packages:
            p = packages[0]
            ids_to_paths_and_dests[df.id] = {
                "path": f"zip_files/{p.filename}",
                "url": df.custom["url"],
            }

    return ids_to_paths_and_dests


def push():
    pass  # Some day...


def pull_destination_files(
    local_package_path: Path,
    product_metadata: md.Metadata,
    destination_id: str,
    *,
    unpackage_zips: bool = False,
):
    """Pull all files for a given destination."""
    dest = product_metadata.get_destination(destination_id)
    if dest.type != BYTES_DEST_TYPE:
        raise Exception(f"{NON_BYTES_DEST_ERROR}. Found: {dest.type}")

    logger.info(f"Pulling BYTES package for {destination_id}")
    ids_to_paths_and_dests = _get_file_url_mappings_by_id(
        product_metadata=product_metadata, destination_id=destination_id
    )
    assemble.make_package_folder(local_package_path)
    product_metadata.write_to_yaml(local_package_path / "metadata.yml")

    package_ids = {p.id for p in product_metadata.assembly}
    for f in dest.files:
        paths_and_dests = ids_to_paths_and_dests[f.id]
        file_path = local_package_path / (paths_and_dests["path"])
        logger.info(f"{local_package_path} - {paths_and_dests['path']} - {file_path}")

        url = paths_and_dests["url"]
        urllib.request.urlretrieve(url, file_path)

        if unpackage_zips and f.id in package_ids:
            logger.info(f"Unpackaging zip: {f.id}")
            assemble.unzip_into_package(
                zip_path=file_path,
                package_path=local_package_path,
                package_id=f.id,
                product_metadata=product_metadata,
            )


def pull_all_destination_files(local_package_path: Path, product_metadata: md.Metadata):
    """Pull all files for all BYTES destinations in the metadata."""
    dests = [d for d in product_metadata.destinations if d.type == BYTES_DEST_TYPE]
    for d in dests:
        pull_destination_files(
            local_package_path, product_metadata, d.id, unpackage_zips=True
        )

    if not local_package_path.exists():
        raise Exception(
            f"The package page {local_package_path} was never created. Likely no files were pulled."
        )
    product_metadata.write_to_yaml(local_package_path / "metadata.yml")


app = typer.Typer()


@app.command("pull_dataset")
def _dataset_from_bytes_cli(
    metadata_path: Path, product: str, version: str, dataset: str
):
    out_dir = assemble.ASSEMBLY_DIR / product / version / dataset
    md_instance = md.Metadata.from_path(
        metadata_path,
        template_vars={"version": version},
    )
    pull_all_destination_files(local_package_path=out_dir, product_metadata=md_instance)


@app.command("pull_product")
def _product_from_bytes_cli(product_metadata_path: Path, product: str, version: str):
    for folder in [
        p for p in product_metadata_path.iterdir() if not p.name.startswith(".")
    ]:
        # assumes the folders == the name of the dataset, which is true. For now.
        _dataset_from_bytes_cli(folder / "metadata.yml", product, version, folder.name)
