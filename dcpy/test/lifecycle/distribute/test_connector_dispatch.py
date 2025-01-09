from pathlib import Path
import pytest
from typing import Any

from dcpy.models.lifecycle.distribution import PublisherPushKwargs
from dcpy.models.product import metadata as md
from dcpy.lifecycle.distribute import dispatcher


@pytest.fixture
def org_metadata(resources_path: Path):
    # TODO: refactor away, into conftest maybe
    template_vars = {
        "version": "24c",
        "lion_prod_level_pub_freq": "monthly",
        "pseudo_lots_pub_freq": "monthly",
        "agency": "fake_agency",
    }
    return md.OrgMetadata.from_path(
        resources_path / "test_product_metadata_repo", template_vars=template_vars
    )


SNOWFLAKE_CONNECTOR_TYPE = "snowflake"


class MockSnowflakeConnector:
    conn_type: str

    def __init__(self):
        self.conn_type = SNOWFLAKE_CONNECTOR_TYPE
        self.push_counter = 0

    def push(
        self,
        thing: PublisherPushKwargs,
    ) -> Any:
        print(thing)
        self.push_counter += 1

    def pull(self, arg: dict) -> Any:
        raise Exception("Pull not implemented for Socrata Connector")


def test_dynamic_dispatch(org_metadata: md.OrgMetadata):
    snowflake_connector = MockSnowflakeConnector()
    dispatcher.register(
        conn_type=SNOWFLAKE_CONNECTOR_TYPE, connector=snowflake_connector
    )
    dispatch_details: PublisherPushKwargs = {
        "metadata": org_metadata.product("lion").dataset("pseudo_lots"),
        "dataset_destination_id": "garlic_sftp",
    }
    assert snowflake_connector.push_counter == 0

    dispatcher.push(SNOWFLAKE_CONNECTOR_TYPE, dispatch_details)

    assert (
        snowflake_connector.push_counter == 1
    ), "The mock snowflake connector should have been called."
