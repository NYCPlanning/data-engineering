from unittest.mock import MagicMock, patch

from pages.connection_checks import helpers


def test_env_vars_report_set_unset_without_values(monkeypatch):
    for var in helpers.REQUIRED_ENV_VARS + helpers.OPTIONAL_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("GHP_TOKEN", "super-secret")
    results = {r.name: r for r in helpers.check_env_vars()}
    assert results["env: GHP_TOKEN"].ok
    assert not results["env: AWS_ACCESS_KEY_ID"].ok
    assert results["env: RECIPES_BUCKET"].ok  # optional
    assert all("super-secret" not in r.detail for r in results.values())


def test_s3_ok_and_failure():
    client = MagicMock()
    ok = helpers.check_s3_bucket("publishing", "bkt", client)
    assert ok.ok and "bkt" in ok.detail
    client.list_objects_v2.assert_called_once_with(Bucket="bkt", MaxKeys=1)
    client.list_objects_v2.side_effect = RuntimeError("blocked")
    bad = helpers.check_s3_bucket("publishing", "bkt", client)
    assert not bad.ok and "blocked" in bad.detail


def test_s3_no_bucket():
    assert not helpers.check_s3_bucket("publishing", None, MagicMock()).ok


def test_postgres_missing_server(monkeypatch):
    monkeypatch.delenv("BUILD_ENGINE_SERVER", raising=False)
    r = helpers.check_postgres()
    assert not r.ok and "BUILD_ENGINE_SERVER" in r.detail


def test_postgres_ok_and_password_scrubbed(monkeypatch):
    monkeypatch.setenv("BUILD_ENGINE_SERVER", "postgresql://u:pw123@dbhost:5432")
    conn = MagicMock()
    with patch("psycopg2.connect", return_value=conn) as connect:
        r = helpers.check_postgres()
    assert r.ok and "dbhost" in r.detail and "pw123" not in r.detail
    assert connect.call_args.kwargs["connect_timeout"] == helpers.TIMEOUT_SECONDS
    conn.close.assert_called_once()

    with patch("psycopg2.connect", side_effect=Exception("bad pw123")):
        r = helpers.check_postgres()
    assert not r.ok and "pw123" not in r.detail


def test_github_ok_unauthorized_and_missing_token(monkeypatch):
    monkeypatch.setenv("GHP_TOKEN", "tok")
    resp = MagicMock(status_code=200)
    resp.json.return_value = {"resources": {"core": {"remaining": 42}}}
    with patch.object(helpers.requests, "get", return_value=resp) as get:
        r = helpers.check_github()
    assert r.ok and "42" in r.detail
    assert get.call_args.kwargs["timeout"] == helpers.TIMEOUT_SECONDS

    with patch.object(helpers.requests, "get", return_value=MagicMock(status_code=401)):
        assert not helpers.check_github().ok

    monkeypatch.delenv("GHP_TOKEN")
    r = helpers.check_github()
    assert not r.ok and "GHP_TOKEN" in r.detail


def test_repo_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(helpers.configuration, "PRODUCT_METADATA_REPO_PATH", tmp_path)
    monkeypatch.setattr(helpers.configuration, "PROJECT_ROOT_PATH", tmp_path)
    results = {r.name: r for r in helpers.check_repo_paths()}
    assert results["path: product-metadata"].ok
    assert not results["path: products dir"].ok
