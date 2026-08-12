import re
from datetime import datetime
from typing import NamedTuple

import pandas as pd
from dateutil.parser import parse as dateutil_parse

from dcpy.lifecycle import product_metadata
from dcpy.lifecycle.connector_registry import connectors


class FuzzyVersion:
    """A version string that supports fuzzy comparison including with various date formats."""

    def __init__(self, version_string):
        self.original = version_string
        self.normalized = self._normalize() if version_string else version_string

    def probably_equals(self, other: "str | FuzzyVersion"):
        fuzzy_other = FuzzyVersion(other) if isinstance(other, str) else other

        if not (self.normalized and fuzzy_other.normalized):
            return False

        return self.normalized == fuzzy_other.normalized

    def _normalize(self):
        """
        Convert various date formats to a standardized form (YYYYMM).

        Returns:
            str: Normalized version string in YYYYMM format, or original if no pattern matches
        """
        if not self.original:
            return self.original

        version = self.original.lower().strip()

        # Handle quarter notation (e.g., "25q1", "24Q2")
        quarter_match = re.match(r"^(\d{2})q([1-4])$", version)
        if quarter_match:
            year_suffix = quarter_match.group(1)
            quarter = int(quarter_match.group(2))
            # Convert 2-digit year to 4-digit (assuming 20XX)
            year = 2000 + int(year_suffix)
            # Quarter to month mapping: Q1=March, Q2=June, Q3=September, Q4=December
            month = quarter * 3
            return f"{year:04d}{month:02d}"

        # Handle YYYYMMDD format
        if re.match(r"^\d{8}$", version):
            return version[:6]  # Take first 6 digits (YYYYMM)

        # Handle YYYYMM format (already in target format)
        if re.match(r"^\d{6}$", version):
            return version

        # Handle month name + year using dateutil, but be selective
        # Only try to parse if it contains month names or reasonable date patterns
        if any(
            month in version
            for month in [
                "january",
                "february",
                "march",
                "april",
                "may",
                "june",
                "july",
                "august",
                "september",
                "october",
                "november",
                "december",
                "jan",
                "feb",
                "mar",
                "apr",
                "may",
                "jun",
                "jul",
                "aug",
                "sep",
                "oct",
                "nov",
                "dec",
            ]
        ):
            try:
                parsed_date = dateutil_parse(
                    version, fuzzy=True, default=datetime(2000, 1, 1)
                )
                # Only return if the parsed date seems reasonable (not the default year)
                if parsed_date.year >= 2000:
                    return f"{parsed_date.year:04d}{parsed_date.month:02d}"
            except (ValueError, TypeError):
                pass

        # Return original if no pattern matches
        return version

    def __str__(self):
        return self.original or ""

    def __repr__(self):
        return f"FuzzyVersion({self.original!r})"

    def __eq__(self, other):
        """Strict equality - delegates to probably_equals for fuzzy comparison."""
        if isinstance(other, FuzzyVersion):
            return self.original == other.original
        return False

    def __hash__(self):
        return hash(self.original)


def open_data_page_url(four_four: str) -> str:
    return f"https://data.cityofnewyork.us/d/{four_four}"


def sort_by_outdated_products(df):
    """
    Sort dataframe to show products with outdated datasets first.
    Products with any outdated datasets appear at the top.
    Also prioritizes products with open_data_versions over those with all blank versions.
    """
    # Create a summary of outdated status by product
    product_status = (
        df.groupby("product")["up_to_date"].agg(["all", "sum", "count"]).reset_index()
    )
    product_status["has_outdated"] = ~product_status["all"]
    product_status["outdated_count"] = product_status["count"] - product_status["sum"]

    # Add flag for products that have any open_data_versions (not all blank/missing)
    product_has_data = (
        df.groupby("product")["open_data_versions"]
        .apply(
            lambda x: x.apply(
                lambda v: bool(v and (v != [] if isinstance(v, list) else True))
            ).any()
        )
        .reset_index()
    )
    product_has_data.columns = ["product", "has_open_data"]
    product_status = product_status.merge(product_has_data, on="product")

    # Sort products:
    # 1. Those with outdated datasets first
    # 2. Those with open data first
    # 3. Then by number of outdated datasets
    product_order = product_status.sort_values(
        ["has_outdated", "has_open_data", "outdated_count"],
        ascending=[False, False, False],
    )["product"].tolist()

    # Reorder the dataframe based on product order
    df_sorted = df.reset_index()
    df_sorted["product_order"] = df_sorted["product"].map(
        {prod: i for i, prod in enumerate(product_order)}
    )
    df_sorted = df_sorted.sort_values(["product_order", "product", "dataset"]).drop(
        "product_order", axis=1
    )

    return df_sorted.set_index(["product", "dataset"])


class MetadataSnapshot(NamedTuple):
    """Everything the comparison needs out of product-metadata, read in a single pass."""

    keys: list[str]
    """Every open_data destination, as product.dataset.destination_id."""
    current_versions: dict[str, str]
    """Per key, the current_version product-metadata declares."""
    four_fours: dict[str, str | None]
    """Per key, the Socrata four-four, for building Open Data page urls."""
    version_tag_in_description: dict[str, bool]
    """Per key, whether the description carries the marker the Open Data version is read back
    out of. Where it doesn't, that dataset's version is unreadable no matter how often it's
    distributed — closing the gap needs a product-metadata edit, not a distribution."""


VERSION_TAG = "Current version:"
"""The marker `connectors.edm.open_data_nyc` greps out of a Socrata description to recover a
dataset's published version."""


def get_metadata_snapshot() -> MetadataSnapshot:
    metadata = product_metadata.load()
    keys = metadata.query_product_dataset_destinations(
        destination_filter={"types": {"open_data"}},
    )
    four_fours = {}
    version_tag_in_description = {}
    for key in keys:
        product, dataset_id, destination_id = key.split(".")
        dataset = metadata.product(product).dataset(dataset_id)
        destination = dataset.get_destination(destination_id)
        four_fours[key] = destination.custom.get("four_four")

        # A destination's files may override the description, so honour an override before
        # looking for the marker — the dataset-level description alone can be misleading.
        description = dataset.attributes.description or ""
        for destination_file in destination.files:
            override = destination_file.dataset_overrides.attributes.description
            if override is not None:
                description = override
                break
        version_tag_in_description[key] = VERSION_TAG in description
    return MetadataSnapshot(
        keys=keys,
        current_versions=get_metadata_versions(metadata),
        four_fours=four_fours,
        version_tag_in_description=version_tag_in_description,
    )


def get_open_data_versions(all_keys):
    open_data_con = connectors["open_data"]
    versions = {}
    for k in all_keys:
        try:
            versions[k] = open_data_con.get_latest_version(k)
        except Exception as e:
            versions[k] = e
    return versions


def get_bytes_versions(all_keys):
    bytes_keys = {k.rsplit(".", 1)[0] for k in all_keys}

    bytes_con = connectors["bytes"]
    versions_by_key = {}
    for k in bytes_keys:
        try:
            versions_by_key[k] = bytes_con.get_latest_version(k)
        except Exception as e:
            versions_by_key[k] = e
    return versions_by_key


def get_metadata_versions(metadata) -> dict[str, str]:
    """Map each product.dataset.destination_id to the current_version product-metadata declares.

    This is the version a distribution run would stamp onto Open Data — the connector reads a
    dataset's Open Data version back out of its rendered description, so `open_data_versions`
    is really this value as of the *last* run, not a fact about the data itself.
    """
    return dict(
        line.split("|", 1)  # type: ignore[misc]
        for line in metadata.get_all_destination_current_versions()
    )


def render_version(value) -> str:
    """Flatten a fetched version into a string for the dataframe.

    The fetchers record a failure as the exception object itself. Left in place those make the
    version columns heterogeneous `object` columns, which Arrow cannot serialize — so anything
    rendering the frame (Streamlit, parquet) fails on a dataset we merely couldn't reach.
    """
    if isinstance(value, Exception):
        return f"error: {type(value).__name__}"
    if value is None or isinstance(value, list) and not value:
        return ""
    return str(value)


def make_comparison_dataframe(metadata_snapshot, bytes_versions, open_data_versions):
    rows = []
    for key in open_data_versions:
        product, dataset, destination_id = key.split(".")
        four_four = metadata_snapshot.four_fours.get(key)
        open_data_url = open_data_page_url(four_four) if four_four else None
        bytes_url = connectors["bytes"].get_page_url(f"{product}.{dataset}")
        raw_bytes_version = bytes_versions.get(f"{product}.{dataset}")
        raw_open_data_vers = open_data_versions.get(key)
        bytes_version = render_version(raw_bytes_version)
        open_data_vers = render_version(raw_open_data_vers)

        metadata_version = metadata_snapshot.current_versions.get(key, "")

        # A failed fetch is never "equal" to anything: two errors of the same type render to the
        # same string, which would otherwise compare equal and read as up to date.
        bytes_ok = not isinstance(raw_bytes_version, Exception)
        open_data_ok = not isinstance(raw_open_data_vers, Exception)

        # Determine if versions are up to date using fuzzy comparison
        up_to_date = False
        if bytes_ok and open_data_ok:
            try:
                up_to_date = FuzzyVersion(bytes_version).probably_equals(
                    FuzzyVersion(open_data_vers)
                )
            except Exception:
                pass

        # Distributing can only close the gap when product-metadata already declares the
        # version that's on Bytes; otherwise the run republishes under the stale version.
        metadata_matches_bytes = False
        if bytes_ok:
            try:
                metadata_matches_bytes = FuzzyVersion(bytes_version).probably_equals(
                    FuzzyVersion(metadata_version)
                )
            except Exception:
                pass

        rows.append(
            {
                "product": product,
                "dataset": dataset,
                "destination_id": destination_id,
                "bytes_version": bytes_version,
                "metadata_version": metadata_version,
                "open_data_versions": open_data_vers,
                "up_to_date": up_to_date,
                "metadata_matches_bytes": metadata_matches_bytes,
                "bytes_version_known": bytes_ok and bool(bytes_version),
                "open_data_version_known": open_data_ok and bool(open_data_vers),
                "open_data_version_readable": metadata_snapshot.version_tag_in_description.get(
                    key, False
                ),
                "bytes_url": bytes_url,
                "open_data_url": open_data_url,
            }
        )
    df = pd.DataFrame(rows).set_index(["product", "dataset"]).sort_index()

    # Add product-level up-to-date flag
    # A product is up-to-date if ALL its datasets are up-to-date
    product_status = df.groupby("product")["up_to_date"].all()
    df["product_up_to_date"] = df.index.get_level_values("product").map(product_status)

    return df


def run():
    metadata_snapshot = get_metadata_snapshot()
    open_data_versions = get_open_data_versions(metadata_snapshot.keys)
    bytes_versions = get_bytes_versions(metadata_snapshot.keys)
    df = make_comparison_dataframe(
        metadata_snapshot, bytes_versions, open_data_versions
    )
    return sort_by_outdated_products(df)
