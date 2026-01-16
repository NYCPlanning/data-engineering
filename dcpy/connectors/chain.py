"""
Connector chaining and resource transfer abstractions.

Enables elegant syntax for transferring resources between connectors:
    builds_conn.resource(key="pluto", build_note="my-build") >> drafts_conn.resource(key="pluto", version="25v1")

The system automatically chooses the most efficient transfer method:
- If both connectors are PathedStorageConnectors on the same cloud: direct copy
- Otherwise: fallback to pull-temp-push pattern
"""

from abc import ABC, abstractmethod
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Dict, Optional, Type

from dcpy.utils.logging import logger

if TYPE_CHECKING:
    from dcpy.connectors.base import Connector
    from dcpy.connectors.pathed_storage import PathedStorageConnector


class Resource:
    """
    Represents a resource within a connector that can be transferred to another connector.

    Examples:
        builds_conn.resource(key="pluto", build_note="my-build")
        drafts_conn.resource(key="pluto", version="25v1", acl="public-read")
        local_conn.resource(path="/tmp/data")
    """

    def __init__(self, connector: "Connector", **resource_spec):
        """
        Initialize a Resource.

        Args:
            connector: The connector containing this resource
            **resource_spec: Connector-specific resource specification (key, version, etc.)
        """
        self.connector = connector
        self.resource_spec = resource_spec

    def __rshift__(self, destination: "Resource") -> "Resource":
        """
        Transfer this resource to another resource using >> operator.

        Args:
            destination: Target resource to transfer to

        Returns:
            The destination resource for chaining
        """
        return transfer_resource(self, destination)

    def __or__(self, destination: "Resource") -> "Resource":
        """
        Transfer this resource to another resource using | operator.

        Args:
            destination: Target resource to transfer to

        Returns:
            The destination resource for chaining
        """
        return transfer_resource(self, destination)

    def __repr__(self) -> str:
        spec_str = ", ".join(f"{k}={v}" for k, v in self.resource_spec.items())
        return f"{self.connector.__class__.__name__}.resource({spec_str})"


class TransferStrategy(ABC):
    """Abstract base class for resource transfer strategies."""

    @abstractmethod
    def can_handle(self, source: Resource, destination: Resource) -> bool:
        """Check if this strategy can handle the transfer."""
        pass

    @abstractmethod
    def transfer(self, source: Resource, destination: Resource) -> Resource:
        """Execute the transfer."""
        pass


class OptimizedCloudTransfer(TransferStrategy):
    """
    Optimized transfer strategy for connectors that support direct transfers.

    Delegates to connector-specific optimization logic rather than making assumptions
    about connector internals.
    """

    def can_handle(self, source: Resource, destination: Resource) -> bool:
        """Check if source connector can optimize transfer to destination connector."""
        source_conn = source.connector
        dest_conn = destination.connector

        # Delegate to connector to determine if it can optimize to the destination
        return hasattr(
            source_conn, "can_optimize_transfer_to"
        ) and source_conn.can_optimize_transfer_to(dest_conn)

    def transfer(self, source: Resource, destination: Resource) -> Resource:
        """Execute optimized transfer via source connector."""
        logger.info(f"Using optimized transfer: {source} -> {destination}")

        # Delegate the actual transfer to the source connector
        source.connector.optimized_transfer_to(
            dest_connector=destination.connector,
            source_spec=source.resource_spec,
            dest_spec=destination.resource_spec,
        )

        logger.info(f"Optimized transfer complete: {source} -> {destination}")
        return destination


class FallbackTempTransfer(TransferStrategy):
    """
    Fallback transfer strategy using temporary directory.

    Downloads from source to temp dir, then uploads to destination.
    Works with any connector types.
    """

    def can_handle(self, source: Resource, destination: Resource) -> bool:
        """This strategy can handle any transfer."""
        return True

    def transfer(self, source: Resource, destination: Resource) -> Resource:
        """Execute fallback temp directory transfer."""
        logger.info(f"Using fallback temp transfer: {source} -> {destination}")

        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Pull from source to temp
            source.connector.pull(destination_path=temp_path, **source.resource_spec)

            # Push from temp to destination
            destination.connector.push(
                source_path=str(temp_path), **destination.resource_spec
            )

        logger.info(f"Fallback transfer complete: {source} -> {destination}")
        return destination


# Registry of transfer strategies, ordered by preference
TRANSFER_STRATEGIES = [
    OptimizedCloudTransfer(),
    FallbackTempTransfer(),  # Always handles as fallback
]


def transfer_resource(source: Resource, destination: Resource) -> Resource:
    """
    Transfer a resource from source to destination using the best available strategy.

    Args:
        source: Source resource to transfer from
        destination: Destination resource to transfer to

    Returns:
        The destination resource for chaining
    """
    # Find the first strategy that can handle this transfer
    for strategy in TRANSFER_STRATEGIES:
        if strategy.can_handle(source, destination):
            return strategy.transfer(source, destination)

    # This should never happen since FallbackTempTransfer always handles
    raise RuntimeError(f"No transfer strategy available for {source} -> {destination}")


def register_transfer_strategy(strategy: TransferStrategy, priority: int = 0):
    """
    Register a new transfer strategy.

    Args:
        strategy: The transfer strategy to register
        priority: Priority (lower = higher priority, 0 = highest)
    """
    TRANSFER_STRATEGIES.insert(priority, strategy)


# Mixin for connectors to add .resource() method
class ResourceMixin:
    """Mixin to add resource() method to connectors."""

    def resource(self, **resource_spec) -> Resource:
        """
        Create a Resource object for this connector.

        Args:
            **resource_spec: Connector-specific resource specification

        Returns:
            Resource object that can be used in transfer chains

        Examples:
            builds_conn.resource(key="pluto", build_note="my-build")
            drafts_conn.resource(key="pluto", version="25v1", acl="public-read")
        """
        return Resource(self, **resource_spec)
