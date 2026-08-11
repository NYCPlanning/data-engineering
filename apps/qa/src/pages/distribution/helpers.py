from typing import NamedTuple

import pandas as pd
import streamlit as st

DISTRIBUTE_REPO = "data-engineering"
DISTRIBUTE_WORKFLOW = "distribute_socrata_from_bytes.yml"
DISTRIBUTE_WORKFLOW_URL = f"https://github.com/NYCPlanning/{DISTRIBUTE_REPO}/actions/workflows/{DISTRIBUTE_WORKFLOW}"
STRINGS_YML_URL = f"https://github.com/NYCPlanning/{DISTRIBUTE_REPO}/blob/main/product-metadata/snippets/strings.yml"


VERSIONS_SCHEMA = 2
"""Bump whenever version_compare's columns change. The disk-persisted cache below keys on this
function's own source, not on version_compare's, so it would otherwise serve a stored dataframe
missing the new columns."""


@st.cache_data(show_spinner=False, persist=True)
def _get_versions(schema: int):
    from dcpy.lifecycle.scripts import version_compare

    return version_compare.run()


def get_versions():
    return _get_versions(VERSIONS_SCHEMA)


class OutdatedGroup(NamedTuple):
    """The datasets of one product that Open Data is behind on, for a single destination."""

    product: str
    destination_id: str
    ready: pd.DataFrame
    """Rows product-metadata already declares at the version on Bytes — distributing publishes it."""
    blocked: pd.DataFrame
    """Rows whose current_version_* in strings.yml is stale — distributing republishes that stale
    version, so the row would still read as outdated afterwards."""


def outdated_groups(versions: pd.DataFrame) -> list[OutdatedGroup]:
    """Group the datasets Open Data is behind on into one workflow dispatch each.

    Grouped by (product, destination_id) rather than by product alone: the distribute CLI
    intersects its `--datasets` and `--destination-ids` filters, so a group spanning several
    destination ids could match up-to-date pairings of the two and redistribute them.
    """
    outdated = versions[~versions["up_to_date"]].reset_index()
    return [
        OutdatedGroup(
            product=str(product),
            destination_id=str(destination_id),
            ready=rows[rows["metadata_matches_bytes"]],
            blocked=rows[~rows["metadata_matches_bytes"]],
        )
        for (product, destination_id), rows in outdated.groupby(
            ["product", "destination_id"]
        )
    ]
