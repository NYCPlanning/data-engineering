import pytest
import dcpy.models.product.dataset.metadata as md

# TODO use test/connectors/socrata/ as inspiration for this test
# TODO differentiate between integration and unit tests when mocking and SFTP server for this


@pytest.mark.skip(reason="SFTP not implemented")
def test_distribute_sftp(metadata: md.Metadata):
    assert False
