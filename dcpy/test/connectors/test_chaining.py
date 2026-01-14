"""
Unit tests for connector chaining and resource transfer optimization.

Tests the automatic optimization of transfers between connectors when they
share the same cloud provider or storage backend.
"""

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, call, patch

import pytest

from dcpy.connectors.chain import (
    FallbackTempTransfer,
    OptimizedCloudTransfer,
    Resource,
    ResourceMixin,
    transfer_resource,
)
from dcpy.connectors.registry import Connector


class MockOptimizedConnector(Connector, ResourceMixin):
    """Mock connector that supports optimized transfers."""

    def __init__(self, name: str, can_optimize: bool = True):
        super().__init__(conn_type="mock")
        self.name = name
        self._can_optimize = can_optimize
        self.optimized_transfer_called = False
        self.pull_called = False
        self.push_called = False

    def can_optimize_transfer_to(self, other_connector: "Connector") -> bool:
        """Mock optimization capability."""
        return (
            self._can_optimize
            and isinstance(other_connector, MockOptimizedConnector)
            and other_connector._can_optimize
        )

    def optimized_transfer_to(
        self, dest_connector: "Connector", source_spec: dict, dest_spec: dict
    ) -> None:
        """Mock optimized transfer."""
        self.optimized_transfer_called = True
        self.last_optimized_call = {
            "dest_connector": dest_connector,
            "source_spec": source_spec,
            "dest_spec": dest_spec,
        }

    def pull(self, destination_path: Path, **kwargs) -> dict:
        """Mock pull method."""
        self.pull_called = True
        self.last_pull_call = {"destination_path": destination_path, "kwargs": kwargs}
        return {"path": destination_path / "mock_file"}

    def push(self, source_path: str, **kwargs) -> dict:
        """Mock push method."""
        self.push_called = True
        self.last_push_call = {"source_path": source_path, "kwargs": kwargs}
        return {"success": True}

    def __repr__(self):
        return f"MockOptimizedConnector({self.name})"


class MockBasicConnector(Connector, ResourceMixin):
    """Mock connector that does not support optimization."""

    def __init__(self, name: str):
        super().__init__(conn_type="mock_basic")
        self.name = name
        self.pull_called = False
        self.push_called = False

    def pull(self, destination_path: Path, **kwargs) -> dict:
        """Mock pull method."""
        self.pull_called = True
        self.last_pull_call = {"destination_path": destination_path, "kwargs": kwargs}
        return {"path": destination_path / "mock_file"}

    def push(self, source_path: str, **kwargs) -> dict:
        """Mock push method."""
        self.push_called = True
        self.last_push_call = {"source_path": source_path, "kwargs": kwargs}
        return {"success": True}

    def __repr__(self):
        return f"MockBasicConnector({self.name})"


class TestResourceTransfer:
    """Test resource transfer between connectors."""

    def test_optimized_transfer_is_used(self):
        """Test that optimized transfer is used when both connectors support it."""
        # Setup
        source_conn = MockOptimizedConnector("source", can_optimize=True)
        dest_conn = MockOptimizedConnector("dest", can_optimize=True)

        source_resource = source_conn.resource(
            key="test_product", build_note="my_build"
        )
        dest_resource = dest_conn.resource(
            key="test_product", version="1.0", acl="public-read"
        )

        # Execute transfer
        result = source_resource >> dest_resource

        # Verify optimized transfer was used
        assert source_conn.optimized_transfer_called, (
            "Optimized transfer should have been called"
        )
        assert not source_conn.pull_called, (
            "Pull should not have been called for optimized transfer"
        )
        assert not dest_conn.push_called, (
            "Push should not have been called for optimized transfer"
        )

        # Verify correct parameters were passed
        assert source_conn.last_optimized_call["dest_connector"] is dest_conn
        assert source_conn.last_optimized_call["source_spec"] == {
            "key": "test_product",
            "build_note": "my_build",
        }
        assert source_conn.last_optimized_call["dest_spec"] == {
            "key": "test_product",
            "version": "1.0",
            "acl": "public-read",
        }

        # Verify return value for chaining
        assert result is dest_resource

    def test_fallback_transfer_is_used(self):
        """Test that fallback transfer is used when optimization is not available."""
        # Setup
        source_conn = MockBasicConnector("source")
        dest_conn = MockBasicConnector("dest")

        source_resource = source_conn.resource(
            key="test_product", build_note="my_build"
        )
        dest_resource = dest_conn.resource(key="test_product", version="1.0")

        # Execute transfer with mocked temp directory
        with patch("dcpy.connectors.chain.TemporaryDirectory") as mock_temp_dir:
            mock_temp_dir.return_value.__enter__.return_value = "/tmp/mock_temp"
            mock_temp_dir.return_value.__exit__.return_value = None

            result = source_resource >> dest_resource

        # Verify fallback transfer was used
        assert source_conn.pull_called, (
            "Pull should have been called for fallback transfer"
        )
        assert dest_conn.push_called, (
            "Push should have been called for fallback transfer"
        )

        # Verify correct parameters were passed
        assert source_conn.last_pull_call["kwargs"] == {
            "key": "test_product",
            "build_note": "my_build",
        }
        assert dest_conn.last_push_call["kwargs"] == {
            "key": "test_product",
            "version": "1.0",
        }

        # Verify return value for chaining
        assert result is dest_resource

    def test_mixed_connectors_use_fallback(self):
        """Test that fallback is used when only one connector supports optimization."""
        # Setup
        source_conn = MockOptimizedConnector("source", can_optimize=True)
        dest_conn = MockBasicConnector("dest")  # No optimization

        source_resource = source_conn.resource(key="test_product")
        dest_resource = dest_conn.resource(key="test_product", version="1.0")

        # Execute transfer
        with patch("dcpy.connectors.chain.TemporaryDirectory") as mock_temp_dir:
            mock_temp_dir.return_value.__enter__.return_value = "/tmp/mock_temp"
            mock_temp_dir.return_value.__exit__.return_value = None

            result = source_resource >> dest_resource

        # Verify fallback was used (not optimization)
        assert not source_conn.optimized_transfer_called, (
            "Optimized transfer should not be called"
        )
        assert source_conn.pull_called, "Pull should have been called for fallback"
        assert dest_conn.push_called, "Push should have been called for fallback"

    def test_chaining_multiple_transfers(self):
        """Test chaining multiple transfers together."""
        # Setup
        conn_a = MockOptimizedConnector("a", can_optimize=True)
        conn_b = MockOptimizedConnector("b", can_optimize=True)
        conn_c = MockOptimizedConnector("c", can_optimize=True)

        resource_a = conn_a.resource(key="test", version="1.0")
        resource_b = conn_b.resource(key="test", version="2.0")
        resource_c = conn_c.resource(key="test", version="3.0")

        # Execute chained transfers
        result = resource_a >> resource_b >> resource_c

        # Verify both transfers used optimization
        assert conn_a.optimized_transfer_called, (
            "First transfer should use optimization"
        )
        assert conn_b.optimized_transfer_called, (
            "Second transfer should use optimization"
        )

        # Verify final result
        assert result is resource_c

    def test_pipe_operator_works(self):
        """Test that | operator works the same as >>."""
        # Setup
        source_conn = MockOptimizedConnector("source", can_optimize=True)
        dest_conn = MockOptimizedConnector("dest", can_optimize=True)

        source_resource = source_conn.resource(key="test")
        dest_resource = dest_conn.resource(key="test", version="1.0")

        # Execute transfer with | operator
        result = source_resource | dest_resource

        # Verify optimized transfer was used
        assert source_conn.optimized_transfer_called, (
            "Optimized transfer should work with | operator"
        )
        assert result is dest_resource

    def test_resource_representation(self):
        """Test that Resource objects have useful string representations."""
        conn = MockOptimizedConnector("test")
        resource = conn.resource(
            key="my_product", version="1.0", build_note="test_build"
        )

        repr_str = repr(resource)

        # Should include connector class name and resource spec
        assert "MockOptimizedConnector" in repr_str
        assert "key=my_product" in repr_str
        assert "version=1.0" in repr_str
        assert "build_note=test_build" in repr_str


class TestTransferStrategies:
    """Test individual transfer strategies."""

    def test_optimized_strategy_can_handle(self):
        """Test OptimizedCloudTransfer strategy detection."""
        strategy = OptimizedCloudTransfer()

        # Setup connectors
        optimized_a = MockOptimizedConnector("a", can_optimize=True)
        optimized_b = MockOptimizedConnector("b", can_optimize=True)
        basic_c = MockBasicConnector("c")

        resource_a = optimized_a.resource(key="test")
        resource_b = optimized_b.resource(key="test")
        resource_c = basic_c.resource(key="test")

        # Test strategy detection
        assert strategy.can_handle(resource_a, resource_b), (
            "Should handle optimized-to-optimized"
        )
        assert not strategy.can_handle(resource_a, resource_c), (
            "Should not handle optimized-to-basic"
        )
        assert not strategy.can_handle(resource_c, resource_a), (
            "Should not handle basic-to-optimized"
        )
        assert not strategy.can_handle(resource_c, resource_c), (
            "Should not handle basic-to-basic"
        )

    def test_fallback_strategy_handles_everything(self):
        """Test that FallbackTempTransfer handles any connector combination."""
        strategy = FallbackTempTransfer()

        # Setup various connector combinations
        optimized_a = MockOptimizedConnector("a")
        basic_b = MockBasicConnector("b")

        resource_a = optimized_a.resource(key="test")
        resource_b = basic_b.resource(key="test")

        # Test that fallback handles everything
        assert strategy.can_handle(resource_a, resource_b)
        assert strategy.can_handle(resource_b, resource_a)
        assert strategy.can_handle(resource_a, resource_a)
        assert strategy.can_handle(resource_b, resource_b)


if __name__ == "__main__":
    pytest.main([__file__])
