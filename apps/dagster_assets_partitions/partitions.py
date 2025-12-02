from typing import Dict
from dagster import DynamicPartitionsDefinition

ingest_partition_def = DynamicPartitionsDefinition(name="ingest_version")

build_partition_defs: Dict[str, DynamicPartitionsDefinition] = {}

# Distribute uses major.minor versioning (e.g., "2025.1", "2025.2", etc.)
# Review now shares partitions with build for simpler workflow
distribute_partition_defs: Dict[str, DynamicPartitionsDefinition] = {}
