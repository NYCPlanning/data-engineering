from datetime import date

from dcpy.utils import git
from dcpy.connectors.edm.publishing import upload

from . import OUTPUT_DIR

PUBLISHING_FOLDER = "db-checkbook"


if __name__ == "__main__":
    print("started export ...")
    with open("version.txt", "w") as f:
        f.write(str(date.today()))
    build_environment = git.branch()
    upload(OUTPUT_DIR, PUBLISHING_FOLDER, build_environment, "public-read")
    print("Finished export!")
