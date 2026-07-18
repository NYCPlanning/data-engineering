"""
Refreshes docs/dcpy/library-to-ingest-status.csv: one row per dataset with a
library and/or ingest template, recording whether it's been migrated to
`ingest_templates/` and whether its `latest` archive in edm-recipes was actually
produced by the ingest pipeline (vs. library, or never archived at all).

Also flags stale_library_files_in_latest: ingest only ever writes `{id}.parquet`
and `config.json` to `latest/`, so any other file there (config.yml, .csv, .sql,
...) is a leftover from a prior library archive that ingest's write never cleared.
Harmless to data products (they only read the parquet) but confusing to a human
browsing S3, since it looks like it could be "the latest data" in another format.

Run manually during library-to-ingest migration work - not scheduled - since it's
meant to be refreshed alongside docs/dcpy/library-to-ingest-status.md, not to track
every archive in real time.
"""

import csv
import json
from pathlib import Path

from dcpy.connectors.edm import recipes
from dcpy.utils import s3
from dcpy.utils.logging import logger

REPO_ROOT = Path(__file__).parent.parent.parent
LIBRARY_TEMPLATES_DIR = REPO_ROOT / "dcpy" / "library" / "templates"
INGEST_TEMPLATES_DIR = REPO_ROOT / "ingest_templates"
OUTPUT_CSV = REPO_ROOT / "docs" / "dcpy" / "library-to-ingest-status.csv"

bucket = recipes._bucket()


def template_ids(dir: Path) -> set[str]:
    return {p.stem for p in dir.glob("*.yml")}


def stale_library_files(dataset_id: str) -> str:
    """Files in latest/ that ingest couldn't have produced (only ever writes
    `{id}.parquet` and `config.json`) - leftovers from a prior library archive
    that ingest's write to latest/ never cleared out.
    """
    prefix = f"{recipes.DATASET_FOLDER}/{dataset_id}/latest/"
    expected = {f"{dataset_id}.parquet", "config.json"}
    filenames = {
        obj["Key"].removeprefix(prefix)
        for obj in s3.list_objects(bucket, prefix)
        if obj["Key"] != prefix
    }
    return ";".join(sorted(filenames - expected))


def latest_status(dataset_id: str) -> dict:
    """Inspect a dataset's latest/config.json to determine archive provenance.

    Ingest-produced configs are top-level {"id": ..., "transformation": ...};
    library-produced configs are top-level {"dataset": {...}, "execution_details": ...}.
    This is a direct structural marker, not an inferred proxy like a timestamp or a
    version-string naming convention (which isn't consistent across datasets - e.g.
    dcp_facilities uses "24v1"-style versions under library too).
    """
    key = f"{recipes.DATASET_FOLDER}/{dataset_id}/latest/config.json"
    if not s3.object_exists(bucket, key):
        return {
            "archived": False,
            "archived_via_ingest": False,
            "latest_version": None,
            "latest_archived_at": None,
            "stale_library_files_in_latest": "",
        }

    config = json.loads(s3.get_file_as_text(bucket, key))
    if "id" in config:
        return {
            "archived": True,
            "archived_via_ingest": True,
            "latest_version": config.get("version"),
            "latest_archived_at": None,
            "stale_library_files_in_latest": stale_library_files(dataset_id),
        }
    else:
        return {
            "archived": True,
            "archived_via_ingest": False,
            "latest_version": config.get("dataset", {}).get("version"),
            "latest_archived_at": config.get("execution_details", {}).get("timestamp"),
            "stale_library_files_in_latest": "",
        }


library_ids = template_ids(LIBRARY_TEMPLATES_DIR)
ingest_ids = template_ids(INGEST_TEMPLATES_DIR)
all_ids = sorted(library_ids | ingest_ids)

logger.info(
    f"{len(library_ids)} library templates, {len(ingest_ids)} ingest templates, "
    f"{len(all_ids)} unique dataset ids"
)

rows = []
for i, dataset_id in enumerate(all_ids, start=1):
    status = latest_status(dataset_id)
    rows.append(
        {
            "dataset_id": dataset_id,
            "migrated_to_ingest": dataset_id in ingest_ids,
            **status,
        }
    )
    if i % 25 == 0 or i == len(all_ids):
        logger.info(f"checked {i}/{len(all_ids)} datasets")

OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
with open(OUTPUT_CSV, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)

logger.info(f"wrote {len(rows)} rows to {OUTPUT_CSV}")
