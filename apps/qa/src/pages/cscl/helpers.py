import pandas as pd
import streamlit as st

from dcpy.configuration import PUBLISHING_BUCKET
from dcpy.connectors.edm.models import BuildKey
from dcpy.lifecycle.builds import builds
from dcpy.utils import s3

PRODUCT = "db-cscl"
PROD_BUCKET = "edm-private"


def get_builds() -> list[str]:
    return builds.list_builds(PRODUCT)


def get_build_version(build: str) -> str:
    return builds.get_build_metadata(PRODUCT, build).version


@st.cache_data(show_spinner=False, ttl=60)
def get_diffs_summary(build: str) -> pd.DataFrame:
    return builds.read_csv(PRODUCT, build, "validation_output/diffs_summary.csv")


@st.cache_data(show_spinner=False, ttl=60)
def get_gdb_comparison(build: str) -> pd.DataFrame:
    """Per-(layer, column) dev-vs-prod comparison of the published LION GDB.

    Written by poc_validation/compare_gdb.py. Raises if the build predates the
    GDB validation step (no such file) — callers should handle that.
    """
    return builds.read_csv(PRODUCT, build, "validation_output/gdb_comparison.csv")


def _find_dataset_file_key(build: str, filename: str) -> str | None:
    """Locate a build file under dataset_files/<group>/ (or dataset_files/ directly)."""
    prefix = f"{BuildKey(PRODUCT, build).path}/dataset_files/"
    for obj in s3.list_objects(PUBLISHING_BUCKET, prefix):
        key = obj["Key"]
        if key.rsplit("/", 1)[-1] == filename:
            return key
    return None


def _lines(data: bytes) -> list[str]:
    return [line for line in data.decode("latin-1").splitlines() if line.strip()]


@st.cache_data(show_spinner=False, ttl=60)
def get_diff_rows(build: str, filename: str) -> list[str]:
    """Return rows present in dev but not prod for a build file.

    Uses the precomputed validation_output/<filename> when the build has one.
    Otherwise finds the file under dataset_files/<group>/ and diffs it against
    the prod copy (equivalent to comm -23 <(sort dev) <(sort prod)).
    """
    try:
        return _lines(builds.get_file(PRODUCT, build, f"validation_output/{filename}"))
    except Exception:
        pass

    key = _find_dataset_file_key(build, filename)
    if key is None:
        raise FileNotFoundError(f"{filename} not found under dataset_files/ in {build}")
    dev_bytes = s3.get_file_as_stream(PUBLISHING_BUCKET, key).read()

    prod_version = get_build_version(build)
    prod_bytes = s3.get_file_as_stream(
        PROD_BUCKET, f"cscl_etl/{prod_version}/{filename}"
    ).read()
    return sorted(set(_lines(dev_bytes)) - set(_lines(prod_bytes)))


def get_build_output_zip_url(build: str) -> str:
    """Return a direct public download URL for the build's output.zip."""
    from dcpy.utils import s3

    build_key = BuildKey(PRODUCT, build)
    bucket = PUBLISHING_BUCKET
    key = f"{build_key.path}/output.zip"
    return s3.get_presigned_get_url(bucket, key)
