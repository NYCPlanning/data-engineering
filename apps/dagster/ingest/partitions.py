"""Partitions for ingest assets: a partition key is a version of the template's dataset.

Templates on a shared version schedule (`version_schedule` in the template) share one static
partitions def of that schedule's versions. Every other template gets its own dynamic def, so
one template's versions don't show up on all the others.
"""

from datetime import date

from dagster import (
    DynamicPartitionsDefinition,
    PartitionsDefinition,
    StaticPartitionsDefinition,
)
from dcpy.lifecycle.ingest.version_schedules import VersionSchedule, versions

# Years the quarterly keys cover: 15a through next year's d. The end moves with the calendar
# each time the code location loads (on deploy or restart).
GEOSUPPORT_FIRST_YEAR = 2015


def geosupport_last_year() -> int:
    return date.today().year + 1


SCHEDULE_PARTITIONS: dict[VersionSchedule, StaticPartitionsDefinition] = {
    VersionSchedule.geosupport_quarterly: StaticPartitionsDefinition(
        versions(
            VersionSchedule.geosupport_quarterly,
            GEOSUPPORT_FIRST_YEAR,
            geosupport_last_year(),
        )
    ),
}


def dynamic_partitions_def_name(template_id: str) -> str:
    return f"ingest_{template_id}"


def ingest_partitions_def(
    template_id: str, version_schedule: VersionSchedule | None = None
) -> PartitionsDefinition:
    if version_schedule:
        return SCHEDULE_PARTITIONS[version_schedule]
    return DynamicPartitionsDefinition(name=dynamic_partitions_def_name(template_id))
