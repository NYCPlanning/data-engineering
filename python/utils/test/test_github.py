import importlib
from unittest.mock import MagicMock, patch

import pytest

from dcpy.utils.git import github


def test_import_without_token(monkeypatch):
    monkeypatch.delenv("GHP_TOKEN", raising=False)
    importlib.reload(github)


def test_missing_token_raises_at_call_time(monkeypatch):
    monkeypatch.delenv("GHP_TOKEN", raising=False)
    with patch.object(github.requests, "get") as get:
        with pytest.raises(RuntimeError, match="GHP_TOKEN is not set"):
            github.get_default_branch("repo")
    get.assert_not_called()


def test_empty_token_raises(monkeypatch):
    monkeypatch.setenv("GHP_TOKEN", "")
    with pytest.raises(RuntimeError, match="GHP_TOKEN is not set"):
        github.get_headers()


def test_token_used_in_header(monkeypatch):
    monkeypatch.setenv("GHP_TOKEN", "abc")
    response = MagicMock()
    response.json.return_value = {"default_branch": "main"}
    with patch.object(github.requests, "get", return_value=response) as get:
        assert github.get_default_branch("repo") == "main"
    assert get.call_args.kwargs["headers"] == {"Authorization": "Bearer abc"}


def test_check_auth_401_message():
    response = MagicMock(status_code=401)
    with pytest.raises(Exception, match="401 Unauthorized"):
        github._check_auth(response)
    github._check_auth(MagicMock(status_code=200))
