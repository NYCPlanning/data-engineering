import os
from pathlib import Path
import shutil
import tempfile

from dcpy.lifecycle import config, product_metadata as org_metadata_loader
from dcpy.lifecycle import data_loader
from dcpy.lifecycle.package import xlsx_writer, validate
from dcpy.models.lifecycle.builds import InputDataset, InputDatasetDestination
from dcpy.models.lifecycle.dataset_event import PackageAssembleResult
import dcpy.models.product.dataset.metadata as md
from dcpy.utils.logging import logger


STAGE = "package.assemble"
ASSEMBLY_DIR = config.local_data_path_for_stage("package.assemble")

ASSEMBLY_INSTRUCTIONS_KEY = "assembly"
METADATA_OVERRIDE_KEY = "with_metadata_from"

SHAPEFILE_SUFFIXES = {"shx", "shp.xml", "shp", "sbx", "sbn", "prj", "dbf", "cpg", "shx"}


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
        f.custom["layer_name"]: f.id for f in package.contents
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
        logger.info("Finished unpacking shapefile.")


def unzip_into_package(
    zip_path: Path,
    package_path: Path,
    package_id: str,
    dataset_metadata: md.Metadata,
):
    """Unpackages a zipped file into our package format.

    Rationale: A product like Pluto will often zip assets (a csv, a readme) into a zip.
    This allows us to reverse that process, to construct packages straight from what's
    zipped up on a destination.
    """
    package = dataset_metadata.get_package(package_id)
    file_ids_to_paths = _file_id_to_zipped_paths(dataset_metadata, package_id)
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


def _get_file_package_paths(
    product_metadata: md.Metadata, destination_id: str
) -> dict[str, str]:
    """Find the destination urls for all destination files and zips in a package."""
    ids_to_paths = {}

    for df in product_metadata.get_destination(destination_id).files:
        # The destination file could be either a `file` or a `package` zip
        files = [f for f in product_metadata.files if f.file.id == df.id]
        packages = [p for p in product_metadata.assembly if p.id == df.id]
        if files:
            f = files[0]
            folder = "attachments" if f.file.is_metadata else "dataset_files"
            ids_to_paths[df.id] = f"{folder}/{f.file.filename}"
        elif packages:
            p = packages[0]
            ids_to_paths[df.id] = f"zip_files/{p.filename}"

    return ids_to_paths


def pull_destination_files(
    product: str,
    dataset: str,
    source_id: str,
    version: str,
    destination_path: Path,
    unpackage_zips: bool = True,
    metadata_only: bool = False,
):
    """Pull all files for a given destination."""

    org_md = org_metadata_loader.load(version=version)
    product_metadata = org_md.product(product).dataset(dataset)
    dest = product_metadata.get_destination(source_id)

    logger.info(f"Pulling package from {source_id}")
    make_package_folder(destination_path)
    product_metadata.write_to_yaml(destination_path / "packaged_metadata.yml")

    id_to_paths = _get_file_package_paths(product_metadata, source_id)

    assembled_file_ids = {p.id for p in product_metadata.assembly}
    logger.info(f"Pulling file ids for assembly: {[f.id for f in dest.files]}")
    for f in dest.files:
        if (
            metadata_only
            and not product_metadata.get_file_and_overrides(f.id).file.is_metadata
        ):
            continue

        ds_id = f"{product}.{dataset}"
        try:
            connector_args = {k: v for k, v in f.custom.items() if k != "key"}
            if "file_id" not in connector_args:
                logger.warning(
                    f"No file_id was provided in connector args for {dest.type}. Using {f.id}"
                )
                connector_args["file_id"] = f.id

            ds = InputDataset(
                id=f.custom.get("key", ds_id),
                version=version,
                source=dest.type,
                destination=InputDatasetDestination.file,
                custom=connector_args,
            )
            file_path = data_loader.pull_dataset(
                ds,
                STAGE,
                dest_dir_override=destination_path / id_to_paths[f.id],
            )
        except Exception as e:
            raise Exception(f"Assembly error Retrieving file {f.id} for {ds_id}: {e}")

        # If the file was a zipped collection of files, or a shapefile with multiple layers
        # Unzip the datset file into their correct spots
        if unpackage_zips and f.id in assembled_file_ids:
            logger.info(f"Unpackaging zip: {f.id}")

            assembled_file = product_metadata.get_package(f.id)
            if assembled_file.type == "zip":
                unzip_into_package(
                    zip_path=file_path,
                    package_path=destination_path,
                    package_id=f.id,
                    dataset_metadata=product_metadata,
                )
            # TODO: move these to constants
            elif assembled_file.type == "multilayer_shapefile":
                unpack_multilayer_shapefile(
                    file_path=file_path,
                    package_path=destination_path,
                    package_id=f.id,
                    dataset_metadata=product_metadata,
                )
            else:
                raise Exception(
                    f"No known method to disassemble type: {assembled_file.type}"
                )


def assemble_dataset_package(
    *,
    product: str,
    dataset: str,
    version: str,
    source_id: str,
    path_override: Path | None = None,
    metadata_only: bool = False,
    validate_dataset_files: bool = False,
) -> PackageAssembleResult:
    assemble_result = lambda **remaining_kwargs: PackageAssembleResult(
        product=product,
        dataset=dataset,
        version=version,
        source_id=source_id,
        **remaining_kwargs,
    )

    org_md = org_metadata_loader.load(version=version)
    dataset_metadata = org_md.product(product).dataset(dataset)

    package_path = (
        (path_override or config.local_data_path_for_stage(STAGE))
        / product
        / dataset
        / version
    )

    try:
        pull_destination_files(
            product,
            dataset,
            source_id,
            version,
            unpackage_zips=True,
            metadata_only=metadata_only,
            destination_path=package_path,
        )
    except Exception as e:
        return assemble_result(
            success=False,
            result_summary="Failed to pull destination files",
            result_details=str(e),
        )

    if validate_dataset_files:
        validation_result = validate.validate(package_path, dataset_metadata)
        if validation_result.has_errors:
            return assemble_result(
                success=False,
                result_summary="Pulled dataset had validation errors.",
                package_path=package_path,
                validation_errors=validation_result.get_errors_list(),
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
            output_path=package_path / "attachments" / f.filename,
        )
    return assemble_result(
        success=True,
        package_path=package_path,
    )
