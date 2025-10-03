from tabulate import tabulate  # type: ignore

from dcpy.connectors.edm.bytes import BytesConnector
from dcpy.utils.logging import logger


def test_bytes_versions_are_retrieved():
    versions = BytesConnector().fetch_all_latest_versions_df()
    with_errors = versions.loc[versions["version_fetch_error"].astype(bool)]
    if not with_errors.empty:
        logger.error(
            tabulate(
                with_errors[["version_fetch_error"]],
                headers="keys",
                tablefmt="presto",
                maxcolwidths=[40, 40],
            )
        )
    assert with_errors.empty, (
        f"""No errors should be when fetching versions, however the following datasets have errors: {str(with_errors.index.values)}. See the logs for details"""
    )
