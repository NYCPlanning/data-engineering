from typing import Protocol, Any, TypeVar, Generic


_O = TypeVar("_O", contravariant=True)
_I = TypeVar("_I", contravariant=True)


class _ConnectorProtocol(Protocol, Generic[_O, _I]):
    conn_type: str

    def push(self, arg: _O, /) -> Any:
        """push"""

    def pull(self, arg: _I, /) -> Any:
        """pull"""


class ConnectorDispatcher(Generic[_O, _I]):
    _connectors: dict[str, _ConnectorProtocol[_O, _I]]

    def __init__(self):
        self._connectors = {}

    def register(self, conn_type: str, connector: _ConnectorProtocol[_O, _I]):
        self._connectors[conn_type] = connector

    def push(self, dest_type: str, arg: _O) -> str:
        if dest_type not in self._connectors:
            raise Exception(f"No connector registered for {dest_type}")
        connector: _ConnectorProtocol = self._connectors[dest_type]
        return connector.push(arg)

    def pull(self, source_type: str, arg: _I) -> str:
        if source_type not in self._connectors:
            raise Exception(f"No connector registered for {source_type}")
        connector: _ConnectorProtocol = self._connectors[source_type]
        return connector.pull(arg)
