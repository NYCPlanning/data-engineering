from datetime import date

# TODO: publishing connector refactor - replace with: from dcpy.lifecycle.builds import builds
from dcpy.connectors.edm import publishing
from dcpy.utils import git

from . import OUTPUT_DIR

PRODUCT = "db-checkbook"


if __name__ == "__main__":
    print("started export ...")
    with open("version.txt", "w") as f:
        f.write(str(date.today()))
    build_environment = git.branch()
    # TODO: publishing connector refactor - replace with: builds.upload(OUTPUT_DIR, PRODUCT, build_environment, acl="public-read")
    publishing.upload_build(
        OUTPUT_DIR, PRODUCT, acl="public-read", build=build_environment
    )
    print("Finished export!")
