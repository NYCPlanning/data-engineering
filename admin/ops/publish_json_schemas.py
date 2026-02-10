import json
from pathlib import Path
from tempfile import TemporaryDirectory

from dcpy.models.lifecycle import ingest as ingest_models
from dcpy.models.product import metadata as product_metadata
from dcpy.models.product.dataset import metadata as dataset_metadata
from dcpy.utils import s3

DO_SCHEMA_FOLDER = "data-engineering-devops/schemas/"

schemas = [
    {
        "name": "org_metadata.schema.json",
        "folder": "product/",
        "schema": product_metadata.OrgMetadataFile.model_json_schema(),
    },
    {
        "name": "product_metadata.schema.json",
        "folder": "product/",
        "schema": product_metadata.ProductMetadataFile.model_json_schema(),
    },
    {
        "name": "dataset_metadata.schema.json",
        "folder": "product/dataset/",
        "schema": dataset_metadata.Metadata.model_json_schema(),
    },
    {
        "name": "ingest_dataset_definition.schema.json",
        "folder": "ingest/",
        "schema": ingest_models.DatasetDefinition.model_json_schema(),
    },
    {
        "name": "ingest_datasource_defintion.schema.json",
        "folder": "ingest/",
        "schema": ingest_models.DataSourceDefinition.model_json_schema(),
    },
]

for schema in schemas:
    with TemporaryDirectory() as _dir:
        p = Path(_dir) / schema["name"]  # type: ignore
        open(p, "w").write(json.dumps(schema["schema"]))
        s3.upload_file(
            bucket="edm-publishing",
            path=p,
            key=DO_SCHEMA_FOLDER + schema["folder"] + schema["name"],  # type: ignore
            acl="public-read",
        )
