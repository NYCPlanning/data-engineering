import os

from boto3 import Session
from cloudpathlib import CloudPath

from dcpy.connectors.hybrid_pathed_storage import HybridPathedStorage


class AzBlobGateway:
    """A bucket behind the S3 gateway in front of Azure Blob (versitygw), configured by
    AZ_BLOB_GATEWAY_ENDPOINT, AZ_BLOB_GATEWAY_ACCESS_KEY_ID and
    AZ_BLOB_GATEWAY_SECRET_ACCESS_KEY."""

    def __init__(self, bucket: str):
        # Explicit credentials and region, so ambient AWS_* env can't leak in.
        session = Session(
            aws_access_key_id=os.environ["AZ_BLOB_GATEWAY_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AZ_BLOB_GATEWAY_SECRET_ACCESS_KEY"],
            region_name="us-east-1",
        )
        root = HybridPathedStorage.Factory.s3(
            s3_bucket=bucket,
            s3_endpoint_url=os.environ["AZ_BLOB_GATEWAY_ENDPOINT"],
            boto3_session=session,
        ).root_path
        assert isinstance(root, CloudPath)
        self.root = root
