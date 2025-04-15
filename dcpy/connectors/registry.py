from __future__ import annotations
from abc import ABC, abstractmethod
from pathlib import Path
from pydantic import BaseModel
from typing import Protocol, Any, TypeVar, Generic, overload

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
class GenericConnector(ABC, BaseModel, extra="forbid"):
    conn_type: str


class Push(GenericConnector, ABC):
    def push(self, key: str, **kwargs) -> Any:
        """Push to a destination that implements versioning."""


class VersionSearch(ABC):
    @abstractmethod
    def list_versions(self, key: str, *, sort_desc: bool = True, **kwargs) -> list[str]:
        pass

    @abstractmethod
    def get_latest_version(self, key: str, **kwargs) -> str:
        pass

    @abstractmethod
    def version_exists(self, key: str, version: str, **kwargs) -> bool:
        pass


class Pull(GenericConnector, ABC):
    def get_latest_version(self, key: str, **kwargs) -> str | None:
        return None

    @abstractmethod
    def pull(self, key: str, destination_path: Path, **kwargs) -> dict:
        """Pull a dataset to the given path"""

    def get_pull_local_sub_path(self, key: str, **kwargs) -> Path:
        """Calculate where the file should be stored locally, e.g. /{key}/{version}/
        Different resources might organize their local resources differently, for example
        if the source for `data.csv` implements drafts per version, the local subpath might be
        /{key}/{version}/{draft}/data.csv
        """
        if "version" in kwargs:
            return (
                Path(key) / kwargs["version"]
            )  # This is maybe a bit more logic than we want here, but seems broadly applicable enough
        else:
            return Path(key)


class Connector(Push, Pull, ABC):
    """A connector that does not version datasets but only stores the "current" or "latest" versions"""


class VersionedConnector(Connector, VersionSearch, ABC):
    """A connector that implements the most standard connector behavior (pull, push, version)"""

    def pull(self, key: str, destination_path: Path, **kwargs) -> dict:
        """Pull a dataset to the given path"""
        return self.pull_versioned(key, destination_path=destination_path, **kwargs)

    def push(self, key: str, **kwargs) -> dict:
        return self.push_versioned(key, **kwargs)

    @abstractmethod
    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        """Pull a dataset to the given path"""

    @abstractmethod
    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        """Push to a destination that implements versioning."""

    @abstractmethod
    def get_latest_version(self, key: str, **kwargs) -> str:
        pass


# TODO - this doesn't really belong. If we moved from key/version to more of a path, this could be done away with
class StorageConnector(Connector, ABC):
    """A connector that does not version datasets but only stores the "current" or "latest" versions"""

    @abstractmethod
    def exists(self, key: str) -> bool:
        return False

    @abstractmethod
    def get_subfolders(self, prefix: str) -> list[str]:
        return []


_C = TypeVar("_C", bound=GenericConnector)
_C2 = TypeVar("_C2", bound=GenericConnector)


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

    @overload
    def __getitem__(self, item: tuple[str, type[_C2]]) -> _C2: ...

    @overload
    def __getitem__(self, item: str) -> _C: ...

    def __getitem__(self, item: str | tuple[str, type[_C2]]) -> _C2 | _C:
        if isinstance(item, str):
            conn_type = item
            type_validator = None
        else:
            conn_type, type_validator = item

        if conn_type not in self._connectors:
            raise Exception(
                f"{self.MISSING_CONN_ERROR_PREFIX} {item}. Registered connectors: {self._connectors.keys()}"
            )

        c = self._connectors[conn_type]
        if type_validator:
            assert isinstance(c, type_validator)
        return c

    def __contains__(self, item):
        return item in self._connectors

    def get_subregistry(self, cls: type[_C2]) -> ConnectorRegistry[_C2]:
        connectors = {
            t: conn for (t, conn) in self._connectors.items() if isinstance(conn, cls)
        }
        return ConnectorRegistry(connectors=connectors)

    @property
    def versioned(self) -> ConnectorRegistry[VersionedConnector]:
        return self.get_subregistry(VersionedConnector)  # type: ignore[type-abstract]

    @property
    def pull(self) -> ConnectorRegistry[Pull]:
        return self.get_subregistry(Pull)  # type: ignore[type-abstract]

    @property
    def push(self) -> ConnectorRegistry[Push]:
        return self.get_subregistry(Push)  # type: ignore[type-abstract]
