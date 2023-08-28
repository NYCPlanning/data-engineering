from datetime import date

from dcpy.connectors.edm.publishing import upload
from dcpy.utils.git import git_branch

from . import OUTPUT_DIR

PUBLISHING_FOLDER = "db-checkbook"


def run_export() -> None:
    build_environment = git_branch()
    version = str(date.today())
    upload(OUTPUT_DIR, PUBLISHING_FOLDER, build_environment, version, "public-read")


if __name__ == "__main__":
    print("started export ...")
    run_export()
    print("Finished export!")
