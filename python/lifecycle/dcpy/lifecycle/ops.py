import shutil
from pathlib import Path

from dcpy.lifecycle import config


def purge_data_dir():
    data_dir: Path = config.CONF["local_data_path"]
    assert data_dir.stem != "data-engineering", (
        "Looks like you're trying to delete the root folder!"
    )
    shutil.rmtree(data_dir)
