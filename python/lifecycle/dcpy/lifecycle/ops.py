import shutil
from pathlib import Path

from dcpy.lifecycle import config


def purge_data_dir():
    # CONF holds the path as a string (it comes from DCPY_LIFECYCLE_DATA_DIR).
    data_dir = Path(config.CONF["local_data_path"])
    assert data_dir.name != "data-engineering", (
        "Looks like you're trying to delete the root folder!"
    )
    shutil.rmtree(data_dir, ignore_errors=not data_dir.exists())
