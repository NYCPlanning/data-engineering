from __future__ import annotations
from abc import ABC
from pathlib import Path
from typing import Protocol, Any, TypeVar, Generic, Type

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
class Connector(ABC):
    conn_type: str


class _VersionedPush(ABC):
    def push(self, key: str, version: str, push_conf: Any | None = None) -> Any:
        """Push to a destination that implements versioning."""


class _NonVersionedPush(ABC):
    def push(self, key: str, push_conf: Any | None = None) -> Any:
        """Push to a destination that does not support explicit versioning"""


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


class _NonVersionedPull(ABC):
    def pull(
        self,
        key: str,
        destination_path: Path,
        pull_conf: Any | None = None,
    ) -> Any:
        """Push from a source that does not support versioning."""

    def get_pull_local_sub_path(
        self,
        key: str,
        pull_conf: Any | None = None,
    ) -> Path:
        """Calculate where the file should be stored locally, e.g. /{key}/{version}/
        Different resources might organize their local resources differently, for example
        if the source for `data.csv` implements drafts per version, the local subpath might be
        /{key}/{version}/{draft}/data.csv
        """
        return Path(key)


class _GetCurrentVersion(ABC):
    def get_version(self, key: str, conf: dict | None = None) -> str | None:
        return None


class _VersionSearch(ABC):
    def list_versions(self, key: str, sort_desc: bool = True) -> list[str]:
        return []

    def query_latest_version(self, key: str, conf: dict | None = None) -> str:
        return ""

    def version_exists(self, key: str, version: str) -> bool:
        return False


class VersionedConnector(Connector, _VersionedPull, _VersionedPush, _VersionSearch):
    """A connector that implements the most standard connector behavior (pull, push, version)"""


class NonVersionedConnector(Connector, _NonVersionedPull, _GetCurrentVersion):
    """A connector that does not version datasets but only stores the "current" or "latest" versions"""


_C = TypeVar("_C", bound=Connector)
_C2 = TypeVar("_C2", bound=Connector)


class ConnectorRegistry(Generic[_C]):
    """A Registry for VersionedConnectors.

    Connectors can be dynamically registered and invoked."""

    MISSING_CONN_ERROR_PREFIX = "No registered connector named:"

    _connectors: dict[str, _C]

    def __init__(self, connectors: dict[str, _C] = {}):
        self._connectors = connectors

    def register(self, connector: _C, *, conn_type: str = ""):
        conn_type = conn_type or connector.conn_type
        logger.info(f"registering {conn_type}")
        self._connectors[conn_type] = connector

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

    def get_subregistry(self, cls: Type[_C2]) -> ConnectorRegistry[_C2]:
        connectors = {
            t: conn for (t, conn) in self._connectors.items() if isinstance(conn, cls)
        }
        print(connectors)
        return ConnectorRegistry(connectors=connectors)  # type: ignore
