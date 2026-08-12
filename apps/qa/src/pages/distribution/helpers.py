import hashlib
from typing import NamedTuple

import pandas as pd
import streamlit as st

DISTRIBUTE_REPO = "data-engineering"
DISTRIBUTE_WORKFLOW = "distribute_socrata_from_bytes.yml"
DISTRIBUTE_WORKFLOW_URL = f"https://github.com/NYCPlanning/{DISTRIBUTE_REPO}/actions/workflows/{DISTRIBUTE_WORKFLOW}"
STRINGS_YML_URL = f"https://github.com/NYCPlanning/{DISTRIBUTE_REPO}/blob/main/product-metadata/snippets/strings.yml"


REQUIRED_COLUMNS = (
    "destination_id",
    "bytes_version",
    "metadata_version",
    "open_data_versions",
    "up_to_date",
    "metadata_matches_bytes",
    "bytes_version_known",
    "open_data_version_known",
    "open_data_version_readable",
)
"""Every column this page reads. Part of the cache key below — see `_schema`."""


def _schema() -> str:
    """Fingerprint of the shapes the persisted caches hold.

    Streamlit keys a cached function on its *own* source, not on `version_compare`'s, so adding
    a column there would otherwise keep serving frames from the previous shape until someone
    remembered to bump a constant by hand. Deriving the key from the shapes themselves means it
    changes exactly when the stored value would no longer fit.
    """
    from dcpy.lifecycle.scripts import version_compare

    parts = REQUIRED_COLUMNS + version_compare.MetadataSnapshot._fields
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:12]


@st.cache_data(show_spinner=False, persist=True)
def _metadata_snapshot(schema: str):
    from dcpy.lifecycle.scripts import version_compare

    return version_compare.get_metadata_snapshot()


@st.cache_data(show_spinner=False, persist=True)
def _open_data_versions(schema: str, keys: list[str]):
    from dcpy.lifecycle.scripts import version_compare

    return version_compare.get_open_data_versions(keys)


@st.cache_data(show_spinner=False, persist=True)
def _bytes_versions(schema: str, keys: list[str]):
    from dcpy.lifecycle.scripts import version_compare

    return version_compare.get_bytes_versions(keys)


@st.cache_data(show_spinner=False, persist=True)
def _comparison(schema: str, keys: list[str], _snapshot, _bytes, _open_data):
    """Keyed on (schema, keys) alone — the underscore-prefixed frames are skipped by Streamlit's
    hasher, and they're already determined by the three cached stages above."""
    from dcpy.lifecycle.scripts import version_compare

    return version_compare.sort_by_outdated_products(
        version_compare.make_comparison_dataframe(_snapshot, _bytes, _open_data)
    )


def get_metadata_snapshot():
    return _metadata_snapshot(_schema())


def get_open_data_versions(keys):
    return _open_data_versions(_schema(), keys)


def get_bytes_versions(keys):
    return _bytes_versions(_schema(), keys)


def compare(snapshot, bytes_versions, open_data_versions):
    return _comparison(
        _schema(), snapshot.keys, snapshot, bytes_versions, open_data_versions
    )


class OutdatedGroup(NamedTuple):
    """The datasets of one product needing attention, for a single destination."""

    product: str
    destination_id: str
    ready: pd.DataFrame
    """Rows product-metadata already declares at the version on Bytes — distributing publishes it."""
    blocked: pd.DataFrame
    """Rows whose current_version_* in strings.yml is stale — distributing republishes that stale
    version, so the row would still read as outdated afterwards."""


class NeedsAttention(NamedTuple):
    """Everything not confirmed up to date, split by what would actually fix it."""

    outdated: list[OutdatedGroup]
    """Both versions read cleanly and differ. Distributing closes the gap."""
    unconfirmed: list[OutdatedGroup]
    """No version could be read off Open Data, but the description carries the marker — so it has
    simply never been stamped (or a fetch failed). Distributing publishes it and makes it
    readable from then on."""
    no_version_tag: pd.DataFrame
    """The description carries no `Current version:` marker at all, so the published version can
    never be read back. Distributing changes nothing; this needs a product-metadata edit."""
    would_lose_version: pd.DataFrame
    """The Open Data page shows a version but product-metadata's description no longer carries the
    marker — so distributing would patch the weaker description over the live one and lose it.
    Drawn from every row, including up-to-date ones, which is exactly where the risk hides."""


def _groups(rows: pd.DataFrame) -> list[OutdatedGroup]:
    """Group into one workflow dispatch each.

    Grouped by (product, destination_id) rather than by product alone: the distribute CLI
    intersects its `--datasets` and `--destination-ids` filters, so a group spanning several
    destination ids could match up-to-date pairings of the two and redistribute them.
    """
    return [
        OutdatedGroup(
            product=str(product),
            destination_id=str(destination_id),
            ready=group[group["metadata_matches_bytes"]],
            blocked=group[~group["metadata_matches_bytes"]],
        )
        for (product, destination_id), group in rows.groupby(
            ["product", "destination_id"]
        )
    ]


def needs_attention(versions: pd.DataFrame) -> NeedsAttention:
    """Bucket the not-up-to-date rows by what would fix them.

    An unread version is not the same as an old one: `open_data_nyc` recovers a version by
    grepping the Socrata description, so a blank means "couldn't read it", not "it's behind".
    """
    rows = versions[~versions["up_to_date"]].reset_index()

    missing = [column for column in REQUIRED_COLUMNS if column not in rows.columns]
    if missing:
        raise KeyError(
            f"Version data is missing {missing}. This usually means a cached frame predates a "
            "column being added — click 'Get versions' to clear the cache and refetch."
        )

    # A version we actually read wins over what metadata claims: a page may carry the marker
    # even where the metadata no longer does.
    comparable = rows["bytes_version_known"] & rows["open_data_version_known"]
    outdated = rows[comparable]
    unreadable = rows[~comparable]

    # Deliberately computed over every row rather than `rows`: a dataset can be perfectly up to
    # date and still be one distribution away from losing the version off its page.
    all_rows = versions.reset_index()
    return NeedsAttention(
        outdated=_groups(outdated),
        unconfirmed=_groups(unreadable[unreadable["open_data_version_readable"]]),
        no_version_tag=unreadable[~unreadable["open_data_version_readable"]],
        would_lose_version=all_rows[
            all_rows["open_data_version_known"]
            & ~all_rows["open_data_version_readable"]
        ],
    )
