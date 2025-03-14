from pathlib import Path
import shutil

from dcpy.lifecycle.connector_registry import NonVersionedConnector


class LocalFileConnector(NonVersionedConnector):
    conn_type = "local_file"

    def pull(
        self, key: str, destination_path: Path, pull_conf: dict | None = None
    ) -> dict:
        if Path(key) != destination_path:
            shutil.copy(key, destination_path)
        return {"path": destination_path}
