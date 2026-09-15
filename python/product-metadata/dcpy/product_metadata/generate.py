from pathlib import Path

from pydantic import BaseModel

from dcpy.product_metadata.models.metadata.org import OrgMetadata
from dcpy.product_metadata.models.metadata.product import (
    DestinationPackageMetadata,
    File,
    Package,
)
from dcpy.product_metadata.writers.oti_xlsx.xlsx_writer import (
    EXCEL_DATA_DICT_METADATA_FILE_TYPE,
    write_xlsx,
)
from dcpy.utils.logging import logger


class GenerateResult(BaseModel):
    product: str
    dataset: str
    success: bool
    files_generated: list[str] = []
    error_message: str | None = None


def generate_metadata_assets(
    org_metadata: OrgMetadata,
    dataset_paths: list[str],
    output_dir: Path,
) -> list[GenerateResult]:
    """Generate metadata assets for dataset paths.

    For each dataset path (product.dataset):
    - Query all dataset files where is_metadata=True
    - Generate XLSX for files with type=oti_data_dictionary
    - Save to {output_dir}/{product}/{dataset}/

    Returns list of results (success/failure per dataset).
    """
    results = []

    for dataset_path in dataset_paths:
        try:
            parts = dataset_path.split(".")
            if len(parts) != 2:
                raise ValueError(
                    f"Invalid dataset path format: {dataset_path}. Expected format: product.dataset"
                )

            product, dataset = parts
            logger.info(f"Generating metadata assets for {dataset_path}")

            product_md = org_metadata.product(product)
            dataset_md = product_md.dataset(dataset)

            files_generated = []
            dataset_output_dir = output_dir / product / dataset
            dataset_output_dir.mkdir(parents=True, exist_ok=True)

            # Create files subfolder for generated metadata files
            files_dir = dataset_output_dir / "files"
            files_dir.mkdir(parents=True, exist_ok=True)

            # Create destinations subfolder for serialized metadata
            destinations_dir = dataset_output_dir / "destinations"
            destinations_dir.mkdir(parents=True, exist_ok=True)

            # Generate metadata files (e.g., XLSX data dictionaries)
            for file_and_overrides in dataset_md.files:
                file_obj = file_and_overrides.file
                if not file_obj.is_metadata:
                    continue

                if file_obj.type == EXCEL_DATA_DICT_METADATA_FILE_TYPE:
                    logger.info(
                        f"Generating XLSX for {dataset_path}: {file_obj.filename}"
                    )
                    output_path = files_dir / file_obj.filename
                    write_xlsx(
                        org_md=org_metadata,
                        product=product,
                        dataset=dataset,
                        output_path=output_path,
                    )
                    files_generated.append(file_obj.filename)
                else:
                    logger.warning(
                        f"Cannot generate metadata file {file_obj.filename} of type '{file_obj.type}' for {dataset_path}"
                    )

            # Serialize destination metadata for each destination
            for destination in dataset_md.destinations:
                dest_id = destination.id
                dest_metadata_dir = destinations_dir / dest_id
                dest_metadata_dir.mkdir(parents=True, exist_ok=True)

                # Build sets for quick lookup
                file_ids = {f.file.id for f in dataset_md.files}
                assembly_ids = {a.id for a in dataset_md.assembly}

                # Find dataset files (not metadata files) to generate metadata for
                dataset_files = []
                for dest_file in destination.files:
                    if dest_file.id in assembly_ids:
                        # It's a package/assembly, include it
                        dataset_files.append(dest_file.id)
                    elif dest_file.id in file_ids:
                        # It's a file, check if it's metadata
                        file_obj = dataset_md.get_file_and_overrides(dest_file.id).file
                        if not file_obj.is_metadata:
                            dataset_files.append(dest_file.id)

                # Collect ALL files for this destination (with overrides applied)
                all_files: list[File | Package] = []
                for dest_file in destination.files:
                    if dest_file.id in file_ids:
                        # It's a file - calculate metadata with overrides
                        file_metadata = dataset_md.calculate_destination_metadata(
                            file_id=dest_file.id, destination_id=dest_id
                        )
                        all_files.append(file_metadata.file)
                    elif dest_file.id in assembly_ids:
                        # It's a package/assembly - add the package itself
                        package = dataset_md.get_package(dest_file.id)
                        all_files.append(package)

                # For each dataset file, create metadata that includes ALL files for this destination
                for dataset_file_id in dataset_files:
                    if dataset_file_id in assembly_ids:
                        # Skip assemblies for now - they need special handling
                        logger.debug(
                            f"Skipping assembly {dataset_file_id} for destination {dest_id}"
                        )
                        continue

                    # Get the dataset metadata for this specific dataset file
                    primary_file_metadata = dataset_md.calculate_destination_metadata(
                        file_id=dataset_file_id, destination_id=dest_id
                    )

                    # Create package metadata with all files
                    package_metadata = DestinationPackageMetadata(
                        dataset=primary_file_metadata.dataset,
                        destination=primary_file_metadata.destination,
                        files=all_files,
                    )

                    # Serialize to YAML (one per dataset file)
                    file_metadata_dir = dest_metadata_dir / dataset_file_id
                    file_metadata_dir.mkdir(parents=True, exist_ok=True)
                    output_yaml_path = file_metadata_dir / "metadata.yml"
                    package_metadata.write_to_yaml(output_yaml_path)
                    logger.info(
                        f"Serialized metadata for {dataset_path}.{dest_id}.{dataset_file_id} (with {len(all_files)} files)"
                    )

            results.append(
                GenerateResult(
                    product=product,
                    dataset=dataset,
                    success=True,
                    files_generated=files_generated,
                )
            )

        except Exception as e:
            logger.error(f"Error generating metadata for {dataset_path}: {e}")
            # Parse as much as we can for the error result
            try:
                product, dataset = dataset_path.split(".")
            except ValueError:
                product, dataset = dataset_path, ""

            results.append(
                GenerateResult(
                    product=product,
                    dataset=dataset,
                    success=False,
                    error_message=str(e),
                )
            )

    return results
