"""Partition parsing and creation utilities."""

from datetime import datetime
from typing import NamedTuple


class BuildPartition(NamedTuple):
    """Parsed build partition components."""

    version: str
    branch: str
    timestamp: str


class IngestPartition(NamedTuple):
    """Parsed ingest partition components."""

    branch: str
    timestamp: str


def parse_build_partition(partition_key: str) -> BuildPartition:
    """Parse build partition key into components.

    Args:
        partition_key: Partition key in format {version}:{timestamp}:{branch}
                      e.g., "26v1.1:20260622T1430:main"

    Returns:
        BuildPartition with version, branch, and timestamp

    Raises:
        ValueError: If partition key format is invalid
    """
    parts = partition_key.split(":")
    if len(parts) != 3:
        raise ValueError(
            f"Invalid build partition format: {partition_key}. "
            f"Expected format: {{version}}:{{timestamp}}:{{branch}}"
        )

    version, timestamp, branch = parts
    return BuildPartition(version=version, branch=branch, timestamp=timestamp)


def parse_ingest_partition(partition_key: str) -> IngestPartition:
    """Parse ingest partition key into components.

    Args:
        partition_key: Partition key in format ingest_{branch}:{timestamp}
                      e.g., "ingest_main:20260622T1430"

    Returns:
        IngestPartition with branch and timestamp

    Raises:
        ValueError: If partition key format is invalid
    """
    if not partition_key.startswith("ingest_"):
        raise ValueError(
            f"Invalid ingest partition format: {partition_key}. "
            f"Expected format: ingest_{{branch}}:{{timestamp}}"
        )

    # Remove 'ingest_' prefix
    remainder = partition_key[7:]  # len("ingest_") = 7
    parts = remainder.split(":")

    if len(parts) != 2:
        raise ValueError(
            f"Invalid ingest partition format: {partition_key}. "
            f"Expected format: ingest_{{branch}}:{{timestamp}}"
        )

    branch, timestamp = parts
    return IngestPartition(branch=branch, timestamp=timestamp)


def create_build_partition(
    version: str, branch: str, timestamp: str | None = None
) -> str:
    """Create a build partition key.

    Args:
        version: Version string (e.g., "26v1.1", "26v2")
        branch: Branch name (e.g., "main", "fix-bug")
        timestamp: Optional timestamp in YYYYMMDDTHHMM format.
                  If not provided, uses current time.

    Returns:
        Partition key in format {version}:{timestamp}:{branch}

    Example:
        >>> create_build_partition("26v1.1", "main")
        "26v1.1:20260622T1430:main"
    """
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%dT%H%M")

    return f"{version}:{timestamp}:{branch}"


def create_ingest_partition(branch: str, timestamp: str | None = None) -> str:
    """Create an ingest partition key.

    Args:
        branch: Branch name (e.g., "main", "fix-geocoding")
        timestamp: Optional timestamp in YYYYMMDDTHHMM format.
                  If not provided, uses current time.

    Returns:
        Partition key in format ingest_{branch}:{timestamp}

    Example:
        >>> create_ingest_partition("main")
        "ingest_main:20260622T1430"
    """
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%dT%H%M")

    return f"ingest_{branch}:{timestamp}"
