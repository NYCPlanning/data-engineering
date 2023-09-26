import pandas as pd

from .scriptor import ScriptorInterface
from . import df_to_tempfile
from dotenv import load_dotenv
import boto3
import os
import io

# Load environmental variables
load_dotenv()

class Scriptor(ScriptorInterface):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def ingest(self) -> pd.DataFrame:
        client = boto3.client(
            "s3",
            aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"],
            endpoint_url = os.environ["AWS_S3_ENDPOINT"]
        )
        obj = client.get_object(
            Bucket="edm-private",
            Key=f"dob_now/dob_now_permits/DOB_Now_Permit_Filing_File_{self.version}.csv",
        )
        data = obj["Body"].read()
        df = pd.read_csv(io.BytesIO(data))
        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path


