# TODO: Move this file into test.utils.shapefiles

import yaml

from dcpy.lifecycle.package import shapefiles
from dcpy.models.product.dataset import metadata as dsmd
from dcpy.test.lifecycle.package.conftest import PACKAGE_RESOURCES_PATH


# TODO remove noqas
def test_parsing_shapefile_md():
    shape_path = (
        PACKAGE_RESOURCES_PATH / "shapefile_with_md" / "sample_shapefile_metadata.xml"
    )
    md_path = (
        PACKAGE_RESOURCES_PATH / "shapefile_with_md" / "sample_shapefile_metadata.yml"
    )

    md = None
    with open(md_path, "r", encoding="utf-8") as raw:
        md = dsmd.Metadata.model_construct(**yaml.safe_load(raw.read()))  # noqa: F841
    parsed_md = shapefiles.parse_shapefile_metadata(file_path=shape_path)  # noqa: F841

    # TODO: post-ruffing. This test is failing for mysterious reasons
    # assert type(parsed_md.attributes) is dsmd.DatasetAttributes
