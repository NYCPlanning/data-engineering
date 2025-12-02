from pathlib import Path
from dagster import ConfigurableResource


class LocalStorageResource(ConfigurableResource):
    base_path: str = ".lifecycle"

    def get_path(self, *parts: str) -> Path:
        path = Path(self.base_path) / Path(*parts)
        path.mkdir(parents=True, exist_ok=True)
        return path


local_storage_resource = LocalStorageResource()
