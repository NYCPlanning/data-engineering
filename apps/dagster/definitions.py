"""Single code location for every Dagster workflow (ingest + builds).

Merges each package's own Definitions, so jobs, schedules, sensors and resources added there show
up here too. workspace.yaml still loads ingest and builds as separate locations for local use; the
runner (apps/docker/entrypoints/dagster-runner.sh) serves this one.
"""

from builds.definitions import defs as builds_defs
from dagster import Definitions
from ingest.definitions import defs as ingest_defs

defs = Definitions.merge(ingest_defs, builds_defs)
