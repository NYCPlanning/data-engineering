from __future__ import annotations
from abc import ABC, abstractmethod
from pathlib import Path
from pydantic import BaseModel
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

    @abstractmethod
    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        """Pull a dataset to the given path"""

    @abstractmethod
    def get_latest_version(self, key: str, **kwargs) -> str:
        pass

    @abstractmethod
    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        """Push to a destination that implements versioning."""



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
