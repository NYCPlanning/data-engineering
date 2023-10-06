from datetime import date

from dcpy.utils import git
from dcpy.connectors.edm import publishing

from . import OUTPUT_DIR

PRODUCT = "db-checkbook"


if __name__ == "__main__":
    print("started export ...")
    with open("version.txt", "w") as f:
        f.write(str(date.today()))
    build_environment = git.branch()
    publishing.upload(
        OUTPUT_DIR, publishing.DraftKey(PRODUCT, build_environment), acl="public-read"
    )
    print("Finished export!")
