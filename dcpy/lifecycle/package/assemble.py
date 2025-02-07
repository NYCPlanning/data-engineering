import os
from pathlib import Path
import urllib.request
import shutil
import tempfile
import typer

from dcpy.configuration import PRODUCT_METADATA_REPO_PATH
from dcpy.lifecycle import WORKING_DIRECTORIES
from dcpy.lifecycle.package import xlsx_writer
import dcpy.models.product.dataset.metadata as md
import dcpy.models.product.metadata as prod_md
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


SHAPEFILE_SUFFIXES = {"shx", "shp.xml", "shp", "sbx", "sbn", "prj", "dbf", "cpg", "shx"}


def unpack_multilayer_shapefile(
    file_path: Path,
    package_path: Path,
    package_id: str,
    dataset_metadata: md.Metadata,
):
    """Unpackages a multilayer shapefile into single layers zips."""
    file_ids_to_paths = _file_id_to_zipped_paths(dataset_metadata, package_id)
    logger.info(f"Unpackaging files: {file_ids_to_paths}")

    make_package_folder(package_path)

    package = dataset_metadata.get_package(package_id)
    package_shapefile_prefixes_to_ids = {
        f.custom["shapefile_file_prefix"]: f.id for f in package.contents
    }

    with tempfile.TemporaryDirectory() as temp_unpacked_dir:
        unpacked_dir = Path(temp_unpacked_dir) / file_path.stem
        logger.info(f"Unpacking shapefile at: {file_path}, to: {unpacked_dir}")
        shutil.unpack_archive(file_path, unpacked_dir)

        # If the file was zipped in a certain way, it may contain an intermediate folder.
        # TODO: make a `dcpy.util` for this
        dir_contents = os.listdir(unpacked_dir)
        if len(dir_contents) == 1 and (unpacked_dir / dir_contents[0]).is_dir():
            unpacked_dir = unpacked_dir / dir_contents[0]

        shapefile_dirs_to_zip = set()
        for fname in os.listdir(unpacked_dir):
            stem, suffix = fname.split(".", maxsplit=1)
            if (
                stem in package_shapefile_prefixes_to_ids
                and suffix in SHAPEFILE_SUFFIXES
            ):
                shapefile_dirs_to_zip.add(stem)
                new_shapefile_dir = unpacked_dir / stem
                new_shapefile_dir.mkdir(exist_ok=True)

                shutil.copy2(
                    (unpacked_dir / fname),  # from
                    new_shapefile_dir / fname,  # to
                )
        for shapefile_dir in list(shapefile_dirs_to_zip):
            shapefile_id = package_shapefile_prefixes_to_ids[shapefile_dir]
            unpackaged_path = str(unpacked_dir / shapefile_dir)

            out_path = package_path / file_ids_to_paths[shapefile_id]["package_path"]
            assert out_path.parent.exists()

            logger.info(f"Packaging shapefile at {unpackaged_path} -> {out_path}")
            shutil.make_archive(
                str(out_path).rsplit(".", 1)[0],
                root_dir=unpackaged_path,
                format="zip",
            )
        print("all done!")
        print(os.listdir(unpacked_dir))


def unzip_into_package(
    zip_path: Path,
    package_path: Path,
    package_id: str,
    product_metadata: md.Metadata,
):
    """Unpackages a zipped file into our package format.

    Rationale: A product like Pluto will often zip assets (a csv, a readme) into a zip.
    This allows us to reverse that process, to construct packages straight from what's
    zipped up on a destination.
    """
    package = product_metadata.get_package(package_id)
    file_ids_to_paths = _file_id_to_zipped_paths(product_metadata, package_id)
    logger.info(f"Unpackaging files: {file_ids_to_paths}")

    make_package_folder(package_path)

    with tempfile.TemporaryDirectory() as temp_unpacked_dir:
        unpacked_dir = Path(temp_unpacked_dir) / zip_path.stem
        logger.info(f"Unpacking zip at: {zip_path}, to: {unpacked_dir}")
        shutil.unpack_archive(zip_path, unpacked_dir)
        assert unpacked_dir.exists(), (
            f"Expected {unpacked_dir} to exist. Found {os.listdir(temp_unpacked_dir)}"
        )
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


def pull_destination_files(
    local_package_path: Path,
    product_metadata: md.Metadata,
    destination_id: str,
    *,
    unpackage_zips: bool = False,
    metadata_only: bool = False,
):
    """Pull all files for a given destination."""
    dest = product_metadata.get_destination(destination_id)

    logger.info(f"Pulling package from {destination_id}")
    ids_to_paths_and_dests = _get_file_url_mappings_by_id(
        product_metadata=product_metadata, destination_id=destination_id
    )
    make_package_folder(local_package_path)
    product_metadata.write_to_yaml(local_package_path / "metadata.yml")

    assembled_file_ids = {p.id for p in product_metadata.assembly}
    for f in dest.files:
        paths_and_dests = ids_to_paths_and_dests[f.id]
        f_is_metadata = (
            f.id in product_metadata.get_file_ids()
            and product_metadata.get_file_and_overrides(f.id).file.is_metadata
        )
        if metadata_only and not f_is_metadata:
            continue
        file_path = local_package_path / (paths_and_dests["path"])
        logger.info(f"{local_package_path} - {paths_and_dests['path']} - {file_path}")

        url = paths_and_dests["url"]

        try:
            # By default, urllib doesn't mention the url in its stack trace, which makes it difficult to debug
            logger.info(f"Retrieving file at url: {url}")
            urllib.request.urlretrieve(url, file_path)
        except Exception:
            raise Exception(f"Assembly error Retrieving file at {url}")

        if unpackage_zips and f.id in assembled_file_ids:
            logger.info(f"Unpackaging zip: {f.id}")

            assembled_file = product_metadata.get_package(f.id)
            if assembled_file.type == "zip":
                unzip_into_package(
                    zip_path=file_path,
                    package_path=local_package_path,
                    package_id=f.id,
                    product_metadata=product_metadata,
                )
            # TODO: move these to constants
            elif assembled_file.type == "multilayer_shapefile":
                unpack_multilayer_shapefile(
                    file_path=file_path,
                    package_path=local_package_path,
                    package_id=f.id,
                    dataset_metadata=product_metadata,
                )
            else:
                raise Exception(
                    f"No known method to disassemble type: {assembled_file.type}"
                )


def pull_destination_package_files(
    *,
    source_destination_id: str,
    local_package_path: Path,
    dataset_metadata: md.Metadata,
):
    """Pull all files for a destination."""
    dests = [
        d for d in dataset_metadata.destinations if d.type == source_destination_id
    ]
    for d in dests:
        pull_destination_files(
            local_package_path, dataset_metadata, d.id, unpackage_zips=True
        )

    if not local_package_path.exists():
        raise Exception(
            f"The package page {local_package_path} was never created. Likely no files were pulled."
        )
    dataset_metadata.write_to_yaml(local_package_path / "metadata.yml")


ASSEMBLY_INSTRUCTIONS_KEY = "assembly"
METADATA_OVERRIDE_KEY = "with_metadata_from"


def assemble_package(
    *,
    org_md: prod_md.OrgMetadata,
    product: str,
    dataset: str,
    version: str,
    source_destination_id: str,
    out_path: Path | None = None,
    metadata_only: bool = False,
) -> Path:
    out_path = out_path or ASSEMBLY_DIR / product / version / dataset
    logger.info(
        f"Assembling dataset from {source_destination_id}. Writing to: {out_path}"
    )

    dataset_metadata = org_md.product(product).dataset(dataset)
    pull_destination_files(
        out_path,
        dataset_metadata,
        source_destination_id,
        unpackage_zips=True,
        metadata_only=metadata_only,
    )

    excel_data_dictionaries = [
        f.file
        for f in dataset_metadata.files
        if f.file.type == xlsx_writer.EXCEL_DATA_DICT_METADATA_FILE_TYPE
    ]
    for f in excel_data_dictionaries:
        # this should eventually be generalized into something that will
        # generate all required missing files, or just running through a list of
        # packaging steps. But for now, it's just the OTI files.
        logger.info(f"Generating OTI XLSX for file {f.filename}")
        xlsx_writer.write_xlsx(
            org_md=org_md,
            product=product,
            dataset=dataset,
            output_path=out_path / "attachments" / f.filename,
        )
    return out_path


app = typer.Typer()


@app.command("assemble_from_source")
def assemble_dataset_cli(
    product: str,
    version: str,
    org_metadata_path: Path = typer.Option(
        PRODUCT_METADATA_REPO_PATH,
        "-z",
        "--metadata-path",
        help="Path to metadata repo. Optionally, set in your env.",
    ),
    dataset: str = typer.Option(
        None,
        "--dataset",
        "-d",
        help="Dataset, if different from product",
    ),
    out_path: Path = typer.Option(
        None,
        "--output-path",
        "-o",
        help="Output Path. Defaults to ./data_dictionary.xlsx",
    ),
    metadata_only: bool = typer.Option(
        False,
        "--output-path",
        "-m",
        help="Only Assemble Metadata.",
    ),
    source_destination_id: str = typer.Option(
        "--source-destination-id",
        "-s",
        help="The Destination which acts as a source for this assembly",
    ),
):
    assert source_destination_id, (
        f"A {source_destination_id} is required to pull files."
    )
    dataset_name = dataset or product
    org_md = prod_md.OrgMetadata.from_path(
        org_metadata_path, template_vars={"version": version}
    )
    assemble_package(
        org_md=org_md,
        product=product,
        dataset=dataset_name,
        source_destination_id=source_destination_id,
        version=version,
        out_path=out_path,
        metadata_only=metadata_only,
    )


@app.command("pull_dataset")
def _pull_dataset_cli(
    product: str,
    version: str,
    source_destination_id: str,
    dataset: str = typer.Option(
        None,
        "--dataset",
        "-d",
        help="Dataset, if different from product",
    ),
    org_metadata_path: Path = typer.Option(
        PRODUCT_METADATA_REPO_PATH,
        "-z",
        "--metadata-path",
        help="Path to metadata repo. Optionally, set in your env.",
    ),
):
    dataset = dataset or product
    org_md = prod_md.OrgMetadata.from_path(
        org_metadata_path, template_vars={"version": version}
    )
    out_dir = ASSEMBLY_DIR / product / version / dataset
    dataset_metadata = org_md.product(product).dataset(dataset)
    pull_destination_package_files(
        source_destination_id=source_destination_id,
        local_package_path=out_dir,
        dataset_metadata=dataset_metadata,
    )
