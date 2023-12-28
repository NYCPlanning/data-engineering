import shutil
from dcpy.connectors.edm import publishing
from dcpy.utils.logging import logger

from . import PRODUCT_S3_NAME, BUILD_NAME, OUTPUT_DIR, PG_CLIENT

METADATA_FILES = [
    "source_data_versions.csv",
    "build_metadata.json",
]
BUILD_TABLES = {
    "templatedb": ["csv"],
}


def export():
    OUTPUT_DIR.mkdir(parents=True)
    # export metadata files
    for filename in METADATA_FILES:
        shutil.copy(OUTPUT_DIR.parent / filename, OUTPUT_DIR / filename)

    # export builds tables
    for table_name, file_types in BUILD_TABLES.items():
        data = PG_CLIENT.get_table(table_name)
        file_path = OUTPUT_DIR / table_name
        for file_type in file_types:
            if file_type == "csv":
                logger.info(
                    f"Exporting table {table_name} as a {file_type} to {OUTPUT_DIR}"
                )
                data.to_csv(file_path.with_suffix(".csv"), index=False)
            else:
                raise NotImplementedError(
                    f"Cannot export a table as file type {file_type}"
                )


def upload():
    logger.info(
        f"Uploading exported files\n\tfrom {OUTPUT_DIR}\n\tto S3 at {publishing.BUCKET}/{PRODUCT_S3_NAME}/draft/{BUILD_NAME}"
    )
    publishing.upload(
        OUTPUT_DIR,
        publishing.DraftKey(PRODUCT_S3_NAME, BUILD_NAME),
        acl="public-read",
    )


if __name__ == "__main__":
    export()
    upload()
