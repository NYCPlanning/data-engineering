from unittest import mock

import paramiko
import pytest

from dcpy.utils import sftp


def _server() -> sftp.SFTPServer:
    return sftp.SFTPServer(
        hostname="host",
        username="user",
        private_key_path="/tmp/key",
        known_hosts_path="/tmp/known_hosts",
    )


@mock.patch.object(sftp.time, "sleep")
@mock.patch.object(sftp.paramiko, "SSHClient")
def test_open_client_retries_then_succeeds(ssh_client, sleep):
    """A transient EOFError (the DOF server dropping the SFTP channel) is retried."""
    client = ssh_client.return_value
    client.connect.side_effect = [EOFError(), EOFError(), None]

    returned_client, _ = _server()._open_client()

    assert returned_client is client
    assert client.connect.call_count == 3
    assert sleep.call_count == 2  # slept before the two retries, not after success


@mock.patch.object(sftp.time, "sleep")
@mock.patch.object(sftp.paramiko, "SSHClient")
def test_open_client_exhausts_attempts(ssh_client, sleep):
    """Persistent failure raises ConnectionError chained from the real cause, so
    Typer's CLI wrapper can't swallow the bare EOFError into 'Aborted.'."""
    ssh_client.return_value.connect.side_effect = EOFError()

    with pytest.raises(ConnectionError) as exc_info:
        _server()._open_client()

    assert isinstance(exc_info.value.__cause__, EOFError)
    assert ssh_client.return_value.connect.call_count == sftp.CONNECT_MAX_ATTEMPTS
    assert sleep.call_count == sftp.CONNECT_MAX_ATTEMPTS - 1


@mock.patch.object(sftp.time, "sleep")
@mock.patch.object(sftp.paramiko, "SSHClient")
def test_open_client_does_not_retry_auth_failure(ssh_client, sleep):
    """Auth failures are deterministic and must surface immediately, not retry."""
    ssh_client.return_value.connect.side_effect = paramiko.AuthenticationException(
        "bad creds"
    )

    with pytest.raises(paramiko.AuthenticationException):
        _server()._open_client()

    assert ssh_client.return_value.connect.call_count == 1
    sleep.assert_not_called()


@mock.patch.object(sftp.time, "sleep")
@mock.patch.object(sftp.paramiko, "SSHClient")
def test_open_client_retries_channel_open_failure(ssh_client, sleep):
    """The observed failure is at open_sftp() (post-auth), so it must be retried too."""
    client = ssh_client.return_value
    client.connect.return_value = None
    client.open_sftp.side_effect = [EOFError(), mock.sentinel.sftp]

    _, channel = _server()._open_client()

    assert channel is mock.sentinel.sftp
    assert client.open_sftp.call_count == 2
    assert sleep.call_count == 1
