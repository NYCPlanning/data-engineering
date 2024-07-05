from pathlib import Path
import zipfile

from dcpy.utils.logging import logger


def unzip(zipped_filename: Path, output_dir: Path) -> list[str]:
    """
    Extracts file(s) from a specified zipped file into an output directory.
    Output directory doesn't need to exist.

    Parameters:
        zipped_filename (Path): Path to the zip archive to be extracted.
        output_dir (Path): Path to the directory where files will be extracted.

    Returns:
        list[str]: A list of filenames of the extracted files.

    Raises:
        AssertionError: If the zip archive does not exist.
    """

    assert (
        zipped_filename.exists()
    ), f"❌ Provided path {zipped_filename} to zipped file wasn't found. Try again"

    with zipfile.ZipFile(zipped_filename, "r") as zip_ref:
        zip_ref.extractall(output_dir)
        extracted_files = zip_ref.namelist()

    logger.info(
        f"✅ Successfully extracted the following file(s) from {zipped_filename}: {extracted_files}"
    )

    return extracted_files
