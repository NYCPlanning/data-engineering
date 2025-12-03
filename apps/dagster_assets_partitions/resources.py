import os
from pathlib import Path
from dagster import ConfigurableResource


class LocalStorageResource(ConfigurableResource):
    base_path: str = ""

    def get_path(self, *parts: str) -> Path:
        if not self.base_path:
            # Use Dagster's storage directory
            dagster_home = os.environ.get("DAGSTER_HOME", Path.home() / ".dagster")
            base = Path(dagster_home) / "storage"
        else:
            base = Path(self.base_path)
        
        path = base / Path(*parts)
        path.mkdir(parents=True, exist_ok=True)
        return path


local_storage_resource = LocalStorageResource()
