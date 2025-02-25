from unittest.mock import patch, Mock
from unittest import TestCase

from dcpy.connectors.edm import publishing
from dcpy.models.connectors.edm.publishing import PublishKey, DraftKey


class TestPublishedConnector(TestCase):
    PRODUCT = "factfinder"
    DATASET = "census"
    conn = publishing.PublishedConnector()

    @patch("dcpy.connectors.edm.publishing.download_file")
    def test_pull(self, download_file):
        PULL_CONF = {"filepath": "folder/data.csv"}
        PRODUCT_DATASET = f"{self.PRODUCT}.{self.DATASET}"

        self.conn.pull(PRODUCT_DATASET, version="123", pull_conf=PULL_CONF)

        expected_path = f"{self.DATASET}/{PULL_CONF['filepath']}"
        download_file.assert_called_once_with(
            PublishKey(product="factfinder", version="123"), expected_path
        )

    @patch("dcpy.connectors.edm.publishing.get_published_versions")
    def test_listing_published_versions(self, get_published_versions):
        self.conn.list_versions(self.PRODUCT)
        get_published_versions.assert_called_once_with(self.PRODUCT)

    @patch("dcpy.connectors.edm.publishing.get_published_versions")
    def test_query_latest(self, get_published_versions):
        VERSIONS = ["1", "2", "3"]
        get_published_versions.side_effect = Mock(return_value=VERSIONS)
        latest = self.conn.query_latest_version(self.PRODUCT)
        assert VERSIONS[-1] == latest

    @patch("dcpy.connectors.edm.publishing.get_published_versions")
    def test_version_exists(self, get_published_versions):
        VERSIONS = ["1", "2", "3"]
        get_published_versions.side_effect = Mock(return_value=VERSIONS)

        assert self.conn.version_exists(self.PRODUCT, version=VERSIONS[0])


class TestDraftsConnector(TestCase):
    conn = publishing.DraftsConnector()
    PRODUCT = "factfinder"
    DATASET = "census"

    @patch("dcpy.connectors.edm.publishing.get_draft_versions")
    def test_listing_draft_versions(self, get_draft_versions):
        PRODUCT = "factfinder"
        self.conn.list_versions(PRODUCT)

        get_draft_versions.assert_called_once_with(PRODUCT)

    @patch("dcpy.connectors.edm.publishing.download_file")
    def test_pull(self, download_file):
        PULL_CONF = {
            "filepath": "folder/data.csv",
            "build": "2-phrase.with:special-chars",
        }
        PRODUCT_DATASET = f"{self.PRODUCT}.{self.DATASET}"

        expected_path = f"{self.DATASET}/{PULL_CONF['filepath']}"
        expected_draft_key = DraftKey(
            self.PRODUCT, version="24v2", revision=PULL_CONF["build"]
        )

        self.conn.pull(
            PRODUCT_DATASET, version=expected_draft_key.version, pull_conf=PULL_CONF
        )

        download_file.assert_called_once_with(expected_draft_key, expected_path)

    @patch("dcpy.connectors.edm.publishing.get_draft_versions")
    def test_query_latest(self, get_draft_versions):
        VERSIONS = ["1", "2", "3"]
        get_draft_versions.side_effect = Mock(return_value=VERSIONS)
        latest = self.conn.query_latest_version(self.PRODUCT)
        assert VERSIONS[-1] == latest

    @patch("dcpy.connectors.edm.publishing.get_draft_versions")
    def test_version_exists(self, get_draft_versions):
        VERSIONS = ["1", "2", "3"]
        get_draft_versions.side_effect = Mock(return_value=VERSIONS)

        assert self.conn.version_exists(self.PRODUCT, version=VERSIONS[0])
