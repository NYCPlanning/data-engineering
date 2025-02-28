from abc import ABC
from pathlib import Path
from typing import Protocol, Any, TypeVar, Generic

from dcpy.utils.logging import logger


### Dispatchers

_O = TypeVar("_O", contravariant=True)
_I = TypeVar("_I", contravariant=True)


class PushPullProtocol(Protocol, Generic[_O, _I]):
    conn_type: str

    def push(self, arg: _O, /) -> Any:
        """push"""

    def pull(self, arg: _I, /) -> Any:
        """pull"""


class ConnectorDispatcher(Generic[_O, _I]):
    _connectors: dict[str, PushPullProtocol[_O, _I]]

    def __init__(self):
        self._connectors = {}

    def register(self, conn_type: str, connector: PushPullProtocol[_O, _I]):
        self._connectors[conn_type] = connector

    def push(self, dest_type: str, arg: _O) -> str:
        if dest_type not in self._connectors:
            raise Exception(f"No connector registered for {dest_type}")
        connector: PushPullProtocol = self._connectors[dest_type]
        return connector.push(arg)

    def pull(self, source_type: str, arg: _I) -> str:
        if source_type not in self._connectors:
            raise Exception(f"No connector registered for {source_type}")
        connector: PushPullProtocol = self._connectors[source_type]
        return connector.pull(arg)


### Connector Base Classes


class _VersionedPush(ABC):
    def push(self, key: str, version: str, push_conf: Any | None = None) -> Any:
        """Push to a destination that implements versioning."""


class _VersionedPull(ABC):
    def pull(
        self,
        key: str,
        version: str,
        destination_path: Path,
        pull_conf: Any | None = None,
    ) -> Any:
        """Push from a source that implements versioning."""

    def get_pull_local_sub_path(
        self,
        key: str,
        version: str,
        pull_conf: Any | None = None,
    ) -> Path:
        """Calculate where the file should be stored locally, e.g. /{key}/{version}/
        Different resources might organize their local resources differently, for example
        if the source for `data.csv` implements drafts per version, the local subpath might be
        /{key}/{version}/{draft}/data.csv
        """
        return Path(key) / version


class _VersionSearch(ABC):
    def list_versions(self, key: str, sort_desc: bool = True) -> list[str]:
        return []

    def query_latest_version(self, key: str) -> str:
        return ""

    def version_exists(self, key: str, version: str) -> bool:
        return False


class VersionedConnector(_VersionedPull, _VersionedPush, _VersionSearch):
    """A connector that implements the most standard connector behavior (pull, push, version)"""

    conn_type: str


class VersionedConnectorRegistry:
    """A Registry for VersionedConnectors.

    Connectors can be dynamically registered and invoked."""

    MISSING_CONN_ERROR_PREFIX = "No registered connector named:"

    _connectors: dict[str, VersionedConnector]

    def __init__(self):
        self._connectors = {}

    def register(self, connector: VersionedConnector, *, conn_type: str = ""):
        logger.info(f"registering {connector.conn_type}")
        self._connectors[connector.conn_type] = connector

    def clear(self):
        self._connectors = {}

    def list_registered(self) -> list[str]:
        return list(self._connectors.keys())

    def __getitem__(self, item):
        if item not in self._connectors:
            raise Exception(
                f"{self.MISSING_CONN_ERROR_PREFIX} {item}. Registered connectors: {self._connectors.keys()}"
            )
        return self._connectors[item]
