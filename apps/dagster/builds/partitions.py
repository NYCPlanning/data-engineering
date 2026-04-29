from dagster import DynamicPartitionsDefinition

build_partition_def = DynamicPartitionsDefinition(name="build_version")
