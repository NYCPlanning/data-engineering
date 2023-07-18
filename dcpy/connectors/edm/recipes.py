import json
from dcpy.connectors import s3


def get_config(name, read_version):
    """Retrieve a recipe config from s3."""
    obj = s3.client().get_object(
        Bucket="edm-recipes", Key=f"datasets/{name}/{read_version}/config.json"
    )
    file_content = str(obj["Body"].read(), "utf-8")
    return json.loads(file_content)
