import pytest

from dcpy.connectors.edm.connectors import EdmConnector
from dcpy.connectors.ingest_datastore import Connector as IngestDatastore
from dcpy.lifecycle import config, connector_registry
from dcpy.lifecycle.builds import connector as builds_connector
from dcpy.lifecycle.ingest import connectors as ingest_connectors

AZ_ENV = {
    "AZ_S3_ENDPOINT": "http://gateway.example.test:7070",
    "AZ_S3_ACCESS_KEY_ID": "az-id",
    "AZ_S3_SECRET_ACCESS_KEY": "az-secret",
}
AZ_NAMES = {
    "az.edm.recipes.datasets",
    "az.edm.recipes.raw_datasets",
    "az.edm.private",
    "az.edm.publishing.builds",
    "az.edm.publishing.drafts",
    "az.edm.publishing.published",
    "az.edm.publishing.plan",
}


@pytest.fixture
def registry(monkeypatch):
    """Rebuild the registry from the current env; restore the defaults afterwards."""
    for var in [*AZ_ENV, *config.CONNECTOR_ENV_VARS.values()]:
        monkeypatch.delenv(var, raising=False)

    def rebuild():
        connector_registry._set_default_connectors()
        return connector_registry.connectors

    yield rebuild
    monkeypatch.undo()
    connector_registry._set_default_connectors()


def _s3_client(conn: IngestDatastore | EdmConnector):
    return conn.storage.storage.root_path.client.client


def test_az_family_absent_by_default(registry):
    names = set(registry().list_registered())
    assert not names & AZ_NAMES
    assert "edm.recipes.datasets" in names


def test_az_family_needs_keys(registry, monkeypatch):
    monkeypatch.setenv("AZ_S3_ENDPOINT", AZ_ENV["AZ_S3_ENDPOINT"])
    assert not set(registry().list_registered()) & AZ_NAMES


def test_az_family_registers_without_network_or_aws_env(registry, monkeypatch):
    """Registration only records where to go; nothing resolves until first use."""
    monkeypatch.delenv("AWS_S3_ENDPOINT", raising=False)
    monkeypatch.setenv("DCPY_S3_ENDPOINT_DENYLIST", "gateway.example.test")
    for k, v in AZ_ENV.items():
        monkeypatch.setenv(k, v)
    assert AZ_NAMES <= set(registry().list_registered())


@pytest.mark.parametrize("name", sorted(AZ_NAMES))
def test_az_connectors_use_their_own_endpoint_and_keys(registry, monkeypatch, name):
    monkeypatch.setenv("AWS_S3_ENDPOINT", "https://nyc3.digitaloceanspaces.com")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "do-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "do-secret")
    monkeypatch.setenv("AZ_PUBLISHING_BUCKET", "az-publishing")
    for k, v in AZ_ENV.items():
        monkeypatch.setenv(k, v)
    conn = registry()[name]
    assert conn.conn_type == name

    client = _s3_client(conn)
    assert client.meta.endpoint_url == AZ_ENV["AZ_S3_ENDPOINT"]
    creds = client._request_signer._credentials
    assert (creds.access_key, creds.secret_key) == ("az-id", "az-secret")
    if isinstance(conn, EdmConnector) and "publishing" in name:
        assert conn.storage.storage.root_path.bucket == "az-publishing"


def test_default_connectors_keep_the_aws_env(registry, monkeypatch):
    monkeypatch.setenv("AWS_S3_ENDPOINT", "https://nyc3.digitaloceanspaces.com")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "do-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "do-secret")
    for k, v in AZ_ENV.items():
        monkeypatch.setenv(k, v)
    client = _s3_client(registry()["edm.recipes.datasets"])
    assert client.meta.endpoint_url == "https://nyc3.digitaloceanspaces.com"
    assert client._request_signer._credentials.access_key == "do-id"


def test_denylist_covers_az_connectors(registry, monkeypatch):
    monkeypatch.setenv("DCPY_S3_ENDPOINT_DENYLIST", "gateway.example.test")
    for k, v in AZ_ENV.items():
        monkeypatch.setenv(k, v)
    from dcpy.utils.s3 import S3ConfigError

    with pytest.raises(S3ConfigError, match="DCPY_S3_ENDPOINT_DENYLIST"):
        registry()["az.edm.publishing.builds"].storage.exists("x")


@pytest.mark.parametrize(
    "getter, env_var, default",
    [
        (
            builds_connector.get_recipes_default_connector,
            "DCPY_RECIPES_CONNECTOR",
            "edm.recipes.datasets",
        ),
        (
            builds_connector.get_builds_default_connector,
            "DCPY_BUILDS_CONNECTOR",
            "edm.publishing.builds",
        ),
        (
            builds_connector.get_drafts_default_connector,
            "DCPY_DRAFTS_CONNECTOR",
            "edm.publishing.drafts",
        ),
        (
            builds_connector.get_published_default_connector,
            "DCPY_PUBLISHED_CONNECTOR",
            "edm.publishing.published",
        ),
        (
            builds_connector.get_plan_default_connector,
            "DCPY_PLAN_CONNECTOR",
            "edm.publishing.plan",
        ),
        (
            ingest_connectors.get_raw_datastore_connector,
            "DCPY_INGEST_RAW_CONNECTOR",
            "edm.recipes.raw_datasets",
        ),
        (
            ingest_connectors.get_processed_datastore_connector,
            "DCPY_INGEST_DATASETS_CONNECTOR",
            "edm.recipes.datasets",
        ),
    ],
)
def test_env_var_overrides_connector(registry, monkeypatch, getter, env_var, default):
    for k, v in AZ_ENV.items():
        monkeypatch.setenv(k, v)
    conns = registry()
    assert getter() is conns[default]

    monkeypatch.setenv(env_var, f"az.{default}")
    assert getter() is conns[f"az.{default}"]
