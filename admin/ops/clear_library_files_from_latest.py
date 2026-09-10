"""
Reports, and optionally clears, library-era files left in ingest datasets' `latest/`
folders in edm-recipes. See issue #2613.

Ingest writes only `{id}.parquet` and `config.json`. Until #2618 it added those to
`latest/` without clearing, so anything an earlier library archive left there survived.
`RECIPE_FILE_TYPE_PREFERENCE` puts pg_dump ahead of parquet and matches on extension
alone, so a leftover `.sql` beside a new parquet is silently preferred.

Datasets fall into three cases, and only the first is safe to fix by clearing:

  clear_latest   `latest/` holds an ingest config and the matching `<version>/` folder
                 is clean. The stale files are copies of an older library archive that
                 still exists in its own version folder, so clearing loses nothing.

  mixed_version  `latest/` holds an ingest config but `<version>/` is mixed too. Caused
                 by `--overwrite`, which replaces only the files ingest writes. Clearing
                 `latest/` would leave `<version>/` still resolving to the `.sql`, so
                 these want a re-ingest at a fresh version instead.

  needs_ingest   `latest/` still holds a library config, so the dataset has a template
                 but has never been archived through ingest. Rename `<version>` to
                 `<version>_library` and re-ingest; nothing to clear.

Run with no arguments to report. Pass --apply to delete, which only ever touches files
in the clear_latest group.
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path

from dcpy.utils import s3
from dcpy.utils.logging import logger

BUCKET = "edm-recipes"
REPO_ROOT = Path(__file__).parent.parent.parent
INGEST_TEMPLATES = REPO_ROOT / "ingest_templates"

# what ingest itself writes; everything else in the folder came from somewhere earlier
INGEST_WRITES = (".parquet",)
CONFIG = "config.json"


def _is_ingest_output(filename: str) -> bool:
    return filename == CONFIG or filename.endswith(INGEST_WRITES)


def _folder_files(prefix: str) -> list[str]:
    return [f for f in s3.get_filenames(BUCKET, prefix) if f and not f.endswith("/")]


def classify(dataset_id: str) -> tuple[str, str | None, list[str]]:
    """Returns (case, version, stale filenames in latest/)."""
    latest = _folder_files(f"datasets/{dataset_id}/latest")
    if not latest:
        return "no_archive", None, []
    stale = sorted(f for f in latest if not _is_ingest_output(f))
    if not stale:
        return "already_clean", None, []
    try:
        raw = (
            s3.client()
            .get_object(Bucket=BUCKET, Key=f"datasets/{dataset_id}/latest/{CONFIG}")[
                "Body"
            ]
            .read()
        )
        config = json.loads(raw)
    except Exception:
        return "unreadable_config", None, stale
    if "id" not in config:  # library configs nest everything under "dataset"
        return "needs_ingest", config.get("dataset", {}).get("version"), stale
    version = config.get("version")
    versioned = _folder_files(f"datasets/{dataset_id}/{version}")
    if all(_is_ingest_output(f) for f in versioned):
        return "clear_latest", version, stale
    return "mixed_version", version, stale


def main(apply: bool) -> None:
    cases: dict[str, list] = defaultdict(list)
    for path in sorted(INGEST_TEMPLATES.glob("*.yml")):
        case, version, stale = classify(path.stem)
        if case not in ("already_clean", "no_archive"):
            cases[case].append((path.stem, version, stale))

    for case in sorted(cases):
        print(f"\n{case} ({len(cases[case])})")
        for dataset_id, version, stale in cases[case]:
            print(f"  {dataset_id:38} {str(version):12} {', '.join(stale)}")

    to_clear = cases.get("clear_latest", [])
    if not apply:
        print(
            f"\nreport only. --apply would delete {sum(len(s) for _, _, s in to_clear)} "
            f"files across {len(to_clear)} datasets in clear_latest."
        )
        return

    for dataset_id, _, stale in to_clear:
        for filename in stale:
            key = f"datasets/{dataset_id}/latest/{filename}"
            logger.info(f"deleting {key}")
            s3.delete(BUCKET, key)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="actually delete")
    main(parser.parse_args().apply)
