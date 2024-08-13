import os
from pathlib import Path
import shutil
import tempfile
import typer

from dcpy.lifecycle import WORKING_DIRECTORIES
import dcpy.models.product.dataset.metadata_v2 as md_v2
from dcpy.utils.logging import logger

ASSEMBLY_DIR = WORKING_DIRECTORIES.packaging / "assembly"


def make_package_folder(path: Path):
    logger.info(f"Making package folders at: {path}")
    path.mkdir(exist_ok=True, parents=True)
    (path / "attachments").mkdir(exist_ok=True)
    (path / "dataset_files").mkdir(exist_ok=True)
    (path / "zip_files").mkdir(exist_ok=True)


def file_id_to_package_paths(
    product_metadata: md_v2.Metadata,
    package_id: str,
) -> dict[str, dict[str, Path]]:
    package = product_metadata.get_package(package_id)
    mapping = {}
    for pf in package.contents:
        f = product_metadata.get_file_and_overrides(pf.id)
        folder = "attachments" if f.file.is_metadata else "dataset_files"
        mapping[f.file.id] = {
            "package_path": Path(folder) / f.file.filename,
            "zipped_path": Path(pf.filename or f.file.filename),
        }
    return mapping


def package(
    output_path: Path,
    package_path: Path,
    package_id: str,
    product_metadata: md_v2.Metadata,
):
    package = product_metadata.get_package(package_id)
    file_ids_to_paths = file_id_to_package_paths(product_metadata, package_id)
    logger.info(f"Packaging files: {file_ids_to_paths}")
    pass  # TODO: the rest


def unpackage(
    package_zip: Path,
    out_path: Path,
    package_id: str,
    product_metadata: md_v2.Metadata,
):
    package = product_metadata.get_package(package_id)
    file_ids_to_paths = file_id_to_package_paths(product_metadata, package_id)
    logger.info(f"Unpackaging files: {file_ids_to_paths}")

    make_package_folder(out_path)

    with tempfile.TemporaryDirectory() as temp_unpacked_dir:
        unpacked_dir = Path(temp_unpacked_dir) / package_zip.stem
        logger.info(f"Unpacking zip at: {package_zip}, to: {unpacked_dir}")
        shutil.unpack_archive(package_zip, unpacked_dir)
        assert (
            unpacked_dir.exists()
        ), f"Expected {unpacked_dir} to exist. Found {os.listdir(temp_unpacked_dir)}"
        logger.info(f"Files in unzipped dir: {os.listdir(unpacked_dir)}")

        if package_zip.stem in os.listdir(unpacked_dir):
            # If the file was zipped in a certain way, it may contain an intermediate folder.
            unpacked_dir = unpacked_dir / package_zip.stem

        for contained_file in package.contents:
            paths = file_ids_to_paths[contained_file.id]
            logger.info(
                f"Extracting zipped file: {contained_file.id}. Path mappings: {paths}"
            )
            copy_from_path = unpacked_dir / str(paths["zipped_path"])
            copy_to_path = out_path / paths["package_path"]
            logger.info(f"Copying {copy_from_path} to {copy_to_path}")
            shutil.copy2(
                copy_from_path,
                copy_to_path,
            )


app = typer.Typer()
