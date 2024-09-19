import os
from pathlib import Path
import shutil
import tempfile
import typer

from dcpy.lifecycle import WORKING_DIRECTORIES
from dcpy.lifecycle.distribute import bytes
from dcpy.lifecycle.package import oti_xlsx
import dcpy.models.product.dataset.metadata_v2 as md
from dcpy.utils.logging import logger

ASSEMBLY_DIR = WORKING_DIRECTORIES.packaging / "assembly"


def make_package_folder(path: Path):
    logger.info(f"Making package folders at: {path}")
    path.mkdir(exist_ok=True, parents=True)
    (path / "attachments").mkdir(exist_ok=True)
    (path / "dataset_files").mkdir(exist_ok=True)
    (path / "zip_files").mkdir(exist_ok=True)


def _file_id_to_zipped_paths(
    product_metadata: md.Metadata,
    package_id: str,
) -> dict[str, dict[str, Path]]:
    """Creates a mapping for locations of
    1. filenames in a zip and
    2. their location in the package.
    We'll have to account for paths in the zip at some point."""
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
    product_metadata: md.Metadata,
):
    _package = product_metadata.get_package(package_id)
    file_ids_to_paths = _file_id_to_zipped_paths(product_metadata, package_id)
    logger.info(f"Packaging files: {file_ids_to_paths}")
    pass  # TODO: One day, the rest


def unzip_into_package(
    zip_path: Path,
    package_path: Path,
    package_id: str,
    product_metadata: md.Metadata,
):
    """Unpackages a zipped file into our package format.

    Rationale: A product like Pluto will often zip assets (a csv, a readme) into a zip.
    This allows us to reverse that process, to construct packages straight from what's
    zipped up on Bytes.
    """
    package = product_metadata.get_package(package_id)
    file_ids_to_paths = _file_id_to_zipped_paths(product_metadata, package_id)
    logger.info(f"Unpackaging files: {file_ids_to_paths}")

    make_package_folder(package_path)

    with tempfile.TemporaryDirectory() as temp_unpacked_dir:
        unpacked_dir = Path(temp_unpacked_dir) / zip_path.stem
        logger.info(f"Unpacking zip at: {zip_path}, to: {unpacked_dir}")
        shutil.unpack_archive(zip_path, unpacked_dir)
        assert (
            unpacked_dir.exists()
        ), f"Expected {unpacked_dir} to exist. Found {os.listdir(temp_unpacked_dir)}"
        logger.info(f"Files in unzipped dir: {os.listdir(unpacked_dir)}")

        if zip_path.stem in os.listdir(unpacked_dir):
            # If the file was zipped in a certain way, it may contain an intermediate folder.
            unpacked_dir = unpacked_dir / zip_path.stem

        for contained_file in package.contents:
            paths = file_ids_to_paths[contained_file.id]
            logger.info(
                f"Extracting zipped file: {contained_file.id}. Path mappings: {paths}"
            )
            copy_from_path = unpacked_dir / str(paths["zipped_path"])
            copy_to_path = package_path / paths["package_path"]
            logger.info(f"Copying {copy_from_path} to {copy_to_path}")
            shutil.copy2(
                copy_from_path,
                copy_to_path,
            )


def assemble_dataset_from_bytes(
    dataset_metadata: md.Metadata,
    *,
    destination_id: str,
    out_path: Path | None,
):
    out_path = out_path or ASSEMBLY_DIR / dataset_metadata.id
    logger.info(f"Assembling dataset from BYTES. Writing to: {out_path}")
    bytes.pull_destination_files(
        out_path, dataset_metadata, destination_id, unpackage_zips=True
    )

    oti_data_dictionaries = [
        f.file
        for f in dataset_metadata.files
        if f.file.type == oti_xlsx.OTI_METADATA_FILE_TYPE
    ]
    for f in oti_data_dictionaries:
        # this should eventually be generalized into something that will
        # generate all required missing files, or just running through a list of
        # packaging steps. But for now, it's just the OTI files.
        logger.info(f"Generating OTI XLSX for file {f.filename}")
        oti_xlsx.write_oti_xlsx(
            metadata_path=out_path / "metadata.yml",
            output_path=out_path / "attachments" / f.filename,
        )


app = typer.Typer()


@app.command("from_bytes")
def assemble_dataset_from_bytes_cli(
    metadata_path: Path,
    destination_id: str,
    out_path: Path = typer.Option(
        None,
        "--output-path",
        "-o",
        help="Output Path. Defaults to ./data_dictionary.xlsx",
    ),
):
    dataset_metadata = md.Metadata.from_path(metadata_path)
    assemble_dataset_from_bytes(
        dataset_metadata, destination_id=destination_id, out_path=out_path
    )
