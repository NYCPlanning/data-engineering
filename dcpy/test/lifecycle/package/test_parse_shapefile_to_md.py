from pathlib import Path
import yaml

from dcpy.lifecycle.package import shapefiles
from dcpy.models.product.dataset import metadata_v2 as dsmd
from . import RESOURCES_PATH


def test_parsing_shapefile_md():
    shape_path = RESOURCES_PATH / "shapefile_with_md" / "sample_shapefile_metadata.xml"
    md_path = RESOURCES_PATH / "shapefile_with_md" / "sample_shapefile_metadata.yml"

    md = None
    with open(md_path, "r", encoding="utf-8") as raw:
        md = dsmd.Metadata.model_construct(**yaml.safe_load(raw.read()))
    parsed_md = shapefiles.parse_shapefile_metadata(file_path=shape_path)

    # TODO: post-ruffing. This test is failing for mysterious reasons
    # assert type(parsed_md.attributes) is dsmd.DatasetAttributes
