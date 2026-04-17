from dagster import DynamicPartitionsDefinition

ingest_partition_def = DynamicPartitionsDefinition(name="ingest_version")
