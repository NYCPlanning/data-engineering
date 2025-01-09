from typing import Any

from dcpy.connectors.ftp import FTPConnector
from dcpy.connectors.socrata import publish as socrata_pub
from dcpy.models.lifecycle.distribution import PublisherPushKwargs

# Sadly, can't use Unpack on kwarg generics yet.
# https://github.com/python/typing/issues/1399


# Wrap the connectors to bind them to the `PublisherPushKwargs`
# so that we can register and delegate calls.
# This is the recommended way for third parties to add custom Distribution Connectors.
class DistributionFTPConnector:
    conn_type: str

    def __init__(self):
        self.conn_type = "ftp"
        self._base_connector = FTPConnector()

    def push(self, arg: PublisherPushKwargs) -> Any:
        md = arg["metadata"]
        dest = md.get_destination(arg["dataset_destination_id"])
        dest_path = dest.custom["destination_path"]
        user_id = dest.custom["user_id"]
        self._base_connector.push(dest_path=dest_path, ftp_profile=user_id)

    def pull(self, _: dict) -> Any:
        raise Exception("Pull is not defined for any Distribution Connectors.")


class SocrataPublishConnector:
    conn_type = "socrata"

    def push(
        self,
        arg: PublisherPushKwargs,
    ) -> Any:
        return socrata_pub.push_dataset(**arg)

    def pull(self, _: dict):
        raise Exception("Pull not implemented for Socrata Connector")
