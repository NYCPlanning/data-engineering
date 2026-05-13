from pathlib import Path

from dagster import ConfigurableResource


class LocalStorageResource(ConfigurableResource):
    """Resource for managing local storage paths for build operations."""

    base_path: str = "/tmp/dagster-builds"

    def get_path(self, *parts: str) -> Path:
        """Get a path under the base storage directory.

        Args:
            *parts: Path components to join under base_path

        Returns:
            Path object for the requested location
        """
        path = Path(self.base_path).joinpath(*parts)
        path.mkdir(parents=True, exist_ok=True)
        return path
