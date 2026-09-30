import pytest

from dcpy.connectors.edm.bytes import _sitemap
from dcpy.connectors.edm.bytes._connector import _parse_latest_version


def test_facilities_urls():
    # facilities
    EXPECTED_CATALOG_URL = "https://www.nyc.gov/assets/planning/json/content/resources/dataset-archives/facilities.json"
    assert EXPECTED_CATALOG_URL == _sitemap.get_dataset_catalog_json_url(
        "facilities", "facilities"
    )

    VERSION = "25v1"
    v25_SHAPEFILE_URL = "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/facilities-database/facilities_25v1_shp.zip"

    assert v25_SHAPEFILE_URL == _sitemap.get_file_url(
        "facilities", "facilities", "shapefile", version=VERSION
    )

    HISTORICAl_VERSION = "24v1"
    HISTORICAL_CSV_URL = f"https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/facilities-database/facilities_{HISTORICAl_VERSION}_csv.zip"

    assert HISTORICAL_CSV_URL == _sitemap.get_file_url(
        "facilities", "facilities", "csv", version=HISTORICAl_VERSION
    )

    DATA_DICT_URL = "https://s-media.nyc.gov/agencies/dcp/assets/files/excel/data-tools/bytes/facilities_datadictionary.xlsx"
    assert DATA_DICT_URL == _sitemap.get_file_url(
        "facilities", "facilities", "data_dictionary"
    )

    PDF_URL = "https://s-media.nyc.gov/agencies/dcp/assets/files/pdf/data-tools/bytes/facilities_readme.pdf"
    assert PDF_URL == _sitemap.get_file_url("facilities", "facilities", "readme")


def test_lion_differences_file():
    # The differences file page on bytes is a prime example of where everything needs an override

    EXPECTED_CATALOG_URL = "https://www.nyc.gov/assets/planning/json/content/resources/dataset-archives/lion-difference-files.json"
    assert EXPECTED_CATALOG_URL == _sitemap.get_dataset_catalog_json_url(
        "lion", "lion_differences_file"
    ), "The catalogue url should be correct"

    EXPECTED_PAGE_URL = "https://apps.nyc.gov/content-api/v1/content/planning/resources/datasets/lion-difference-files"
    assert EXPECTED_PAGE_URL == _sitemap.get_most_recent_version_url(
        "lion", "lion_differences_file"
    ), "The page url should be correct"

    VERSION = "25b"
    EXPECTED_FILE_URL = f"https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/lion-differences-file/ldf_{VERSION}.zip"

    assert EXPECTED_FILE_URL == _sitemap.get_file_url(
        "lion", "lion_differences_file", "shapefile", version=VERSION
    )


def _version_block(*lines):
    spans = "".join(
        f'<span class="display--block copy--lg">{line}</span>' for line in lines
    )
    return f'<p class="copy--lg">{spans}</p>'


@pytest.mark.parametrize(
    "lines, expected",
    [
        (["Latest Release: 26Q2.1", "Date of Data: June 2026"], "26q2.1"),
        (["Latest Version: 26Q2.1", "Latest Release: June 2026"], "26q2.1"),
        (["Latest Release: April 2026", "Date of Data: April 2026"], "april 2026"),
    ],
)
def test_parse_latest_version(lines, expected):
    assert _parse_latest_version(_version_block(*lines)) == expected


def test_parse_latest_version_without_label():
    with pytest.raises(AssertionError):
        _parse_latest_version(_version_block("Date of Data: June 2026"))
