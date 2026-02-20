import re
from datetime import datetime

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


def get_all_open_data_keys() -> list[str]:
    """retrieve all product.dataset.destination_ids"""
    return product_metadata.load(version="dummy").query_product_dataset_destinations(
        destination_filter={"types": {"open_data"}},
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


def make_comparison_dataframe(bytes_versions, open_data_versions):
    rows = []
    for key in open_data_versions:
        product, dataset, destination_id = key.split(".")
        bytes_version = bytes_versions.get(f"{product}.{dataset}")
        open_data_vers = open_data_versions.get(key, [])
        open_data_con = connectors["open_data"]
        # socrata_dest = soc_pub.SocrataDestination(metadata, dataset_destination_id)
        four_four = "idk_yet"
        open_data_url = open_data_page_url(four_four)


        # Determine if versions are up to date using fuzzy comparison
        up_to_date = False
        try:
            up_to_date = FuzzyVersion(bytes_version).probably_equals(
                FuzzyVersion(open_data_vers)
            )
        except Exception:
            pass

        rows.append(
            {
                "product": product,
                "dataset": dataset,
                "destination_id": destination_id,
                "bytes_version": bytes_version,
                "open_data_versions": open_data_vers,
                "up_to_date": up_to_date,
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
    all_keys = get_all_open_data_keys()
    open_data_versions = get_open_data_versions(all_keys)
    bytes_versions = get_bytes_versions(all_keys)
    df = make_comparison_dataframe(bytes_versions, open_data_versions)
    return sort_by_outdated_products(df)
