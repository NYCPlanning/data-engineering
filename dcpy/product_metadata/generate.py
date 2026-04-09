from pathlib import Path

from pydantic import BaseModel

from dcpy.product_metadata.models.metadata.org import OrgMetadata
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

            # Get all metadata files at dataset level
            metadata_files = [
                f
                for f in dataset_md.files
                if f.file.is_metadata
                and f.file.type == EXCEL_DATA_DICT_METADATA_FILE_TYPE
            ]

            if not metadata_files:
                logger.info(f"No XLSX metadata files found for {dataset_path}")

            files_generated = []
            dataset_output_dir = output_dir / product / dataset
            dataset_output_dir.mkdir(parents=True, exist_ok=True)

            # Create files subfolder for XLSX outputs
            files_dir = dataset_output_dir / "files"
            files_dir.mkdir(parents=True, exist_ok=True)

            # Create destinations subfolder for serialized metadata
            destinations_dir = dataset_output_dir / "destinations"
            destinations_dir.mkdir(parents=True, exist_ok=True)

            # Generate XLSX files (only if there are any)
            for file_and_overrides in metadata_files:
                file_obj = file_and_overrides.file

                logger.info(f"Generating XLSX for {dataset_path}: {file_obj.filename}")
                output_path = files_dir / file_obj.filename

                write_xlsx(
                    org_md=org_metadata,
                    product=product,
                    dataset=dataset,
                    output_path=output_path,
                )
                files_generated.append(file_obj.filename)

            # Serialize destination metadata for each destination
            for destination in dataset_md.destinations:
                dest_id = destination.id
                dest_metadata_dir = destinations_dir / dest_id
                dest_metadata_dir.mkdir(parents=True, exist_ok=True)

                # For each file/package that the destination references
                for dest_file in destination.files:
                    # Skip metadata files (e.g., xlsx data dictionaries, pdfs, etc.)
                    try:
                        file_obj = dataset_md.get_file_and_overrides(dest_file.id).file
                        if file_obj.is_metadata:
                            logger.debug(
                                f"Skipping metadata file {dest_file.id} for destination serialization"
                            )
                            continue
                    except Exception:
                        # If it's a package/assembly, we can't get_file_and_overrides
                        # Just try to serialize and let it fail gracefully below
                        pass

                    try:
                        # Calculate destination metadata with overrides
                        dest_metadata = dataset_md.calculate_destination_metadata(
                            file_id=dest_file.id, destination_id=dest_id
                        )

                        # Serialize to YAML (one per file in its own folder)
                        file_metadata_dir = dest_metadata_dir / dest_file.id
                        file_metadata_dir.mkdir(parents=True, exist_ok=True)
                        output_yaml_path = file_metadata_dir / "metadata.yml"
                        dest_metadata.write_to_yaml(output_yaml_path)
                        logger.info(
                            f"Serialized metadata for {dataset_path}.{dest_id}.{dest_file.id}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Could not serialize metadata for {dataset_path}.{dest_id}.{dest_file.id}: {e}"
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
