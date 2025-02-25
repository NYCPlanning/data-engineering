from pathlib import Path
from pydantic import BaseModel
from typing import Protocol, Any, TypeVar, Generic

from dcpy.utils.logging import logger

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


class VersionedPushPullProtocol(BaseModel):
    def push(self, key: str, version: str, push_conf: Any | None = None) -> Any:
        """push"""

    def pull(
        self,
        key: str,
        version: str,
        destination_path: Path,
        pull_conf: Any | None = None,
    ) -> Any:
        """pull"""


class VersionSearchProtocol(BaseModel):
    def list_versions(self, key: str, sort_desc: bool = True) -> list[str]:
        return []

    def query_latest_version(self, key: str) -> str:
        return ""

    def version_exists(self, key: str, version: str) -> bool:
        return False


class VersionedConnector(VersionedPushPullProtocol, VersionSearchProtocol):
    conn_type: str


class VersionedConnectorRegistry:
    MISSING_CONN_ERROR_PREFIX = "No registered connector named:"

    _connectors: dict[str, VersionedConnector]

    def __init__(self):
        self._connectors = {}

    def register(self, connector: VersionedConnector, *, conn_type: str = ""):
        logger.info(f"registering {connector.conn_type}")
        self._connectors[connector.conn_type] = connector

    def list_registered(self) -> list[str]:
        return list(self._connectors.keys())

    def __getitem__(self, item):
        if item not in self._connectors:
            raise Exception(
                f"{self.MISSING_CONN_ERROR_PREFIX} {item}. Registered connectors: {self._connectors.keys()}"
            )
        return self._connectors[item]
