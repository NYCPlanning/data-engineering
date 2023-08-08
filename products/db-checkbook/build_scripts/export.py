from datetime import date
from pathlib import Path
from dcpy.connectors.edm.publishing import upload
from dcpy.git import git_branch

from . import (
    OUTPUT_DIR,
    SOURCE_DATA_VERSIONS_FILENAME,
    BUILD_OUTPUT_FILENAME,
    SUMMARY_STATS_DESCRIBE_FILENAME,
    SUMMARY_STATS_LOG_FILENAME,
)

PUBLISHING_FOLDER = "db-checkbook"


def run_export() -> None:
    build_environment = git_branch()
    version = str(date.today())
    file_paths = [
        Path(OUTPUT_DIR.name) / SOURCE_DATA_VERSIONS_FILENAME,
        Path(OUTPUT_DIR.name) / BUILD_OUTPUT_FILENAME,
        Path(OUTPUT_DIR.name) / SUMMARY_STATS_DESCRIBE_FILENAME,
        Path(OUTPUT_DIR.name) / SUMMARY_STATS_LOG_FILENAME,
    ]
    for file_path in file_paths:
        print(
            f"exporting file {file_path} to {build_environment} / {version} / {file_path}"
        )
        upload(
            output=file_path,
            publishing_folder=PUBLISHING_FOLDER,
            version=version,
            acl="public-read",
            s3_subpath=build_environment,
        )


if __name__ == "__main__":
    print("started export ...")
    run_export()
    print("Finished export!")
