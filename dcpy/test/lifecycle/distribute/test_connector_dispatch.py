import pytest
from typing import Any

from dcpy.models.lifecycle.distribute import DatasetDestinationPushArgs
from dcpy.models.product import metadata as md
from dcpy.lifecycle.distribute import dispatcher


@pytest.fixture
def org_metadata(package_and_dist_test_resources):
    return package_and_dist_test_resources.org_md


SNOWFLAKE_CONNECTOR_TYPE = "snowflake"


class MockSnowflakeConnector:
    conn_type: str

    def __init__(self):
        self.conn_type = SNOWFLAKE_CONNECTOR_TYPE
        self.push_counter = 0

    def push(
        self,
        thing: DatasetDestinationPushArgs,
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
    dispatch_details: DatasetDestinationPushArgs = {
        "metadata": org_metadata.product("lion").dataset("pseudo_lots"),
        "dataset_destination_id": "garlic_sftp",
    }
    assert snowflake_connector.push_counter == 0

    dispatcher.push(SNOWFLAKE_CONNECTOR_TYPE, dispatch_details)

    assert snowflake_connector.push_counter == 1, (
        "The mock snowflake connector should have been called."
    )
