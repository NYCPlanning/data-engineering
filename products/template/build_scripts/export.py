from dcpy.lifecycle import product_metadata
from dcpy.product_metadata.writers import pdf_writer, yaml_writer
from dcpy.product_metadata.writers.oti_xlsx import xlsx_writer
from dcpy.utils.logging import logger

from . import get_output_dir


def generate_data_dictionaries():
    """Generate data dictionary files (yml, pdf, xlsx) into build output attachments folder.

    These files will be copied to the final output location by dcpy's export function.
    """
    # Create attachments subdirectory in build output directory
    output_dir = get_output_dir()
    attachments_dir = output_dir / "attachments"
    attachments_dir.mkdir(parents=True, exist_ok=True)

    org_metadata = product_metadata.load(version="DUMMY VERSION")

    logger.info(f"Generating data dictionaries to {attachments_dir}")

    yaml_writer.write_yaml(
        org_metadata=org_metadata,
        product="template_db",
        output_path=attachments_dir / "data_dictionary.yml",
    )
    pdf_writer.write_pdf(
        org_metadata=org_metadata,
        product="template_db",
        output_path=attachments_dir / "data_dictionary.pdf",
    )
    xlsx_writer.write_xlsx(
        org_md=org_metadata,
        product="template_db",
        output_path=attachments_dir / "data_dictionary.xlsx",
    )


def generate_socrata_package():
    """Generate socrata-specific files in build output directory.

    Creates dataset_files folder with the shapefile.
    The attachments folder already has data dictionaries from generate_data_dictionaries().
    These will be copied to the final output by dcpy's export function.
    """
    # Create dataset_files folder in build output directory
    output_dir = get_output_dir()
    dataset_files_dir = output_dir / "dataset_files"
    dataset_files_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Generating socrata package files in {output_dir}")

    # Note: OTI XLSX and other data dictionaries already created by generate_data_dictionaries()
    # Note: source_data_versions.csv will be copied by dcpy's export function

    # We'll create a placeholder for the shapefile - the actual file will be
    # copied by export from the data folder
    logger.info(
        "Socrata-specific files prepared (shapefile will be copied during export)"
    )


if __name__ == "__main__":
    generate_data_dictionaries()
