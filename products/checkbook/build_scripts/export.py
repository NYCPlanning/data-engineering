from datetime import date

from dcpy.utils import git
from dcpy.connectors.edm.publishing import upload

from . import OUTPUT_DIR

PUBLISHING_FOLDER = "db-checkbook"


def run_export() -> None:
    build_environment = git.branch()
    version = str(date.today())
    upload(OUTPUT_DIR, PUBLISHING_FOLDER, build_environment, version, "public-read")


if __name__ == "__main__":
    print("started export ...")
    run_export()
    print("Finished export!")
