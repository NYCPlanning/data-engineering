from pathlib import Path
from dcpy.lifecycle.package import validate

COLP_PACKAGE_PATH = (
    Path(__file__).parent.resolve() / "resources" / "colp_single_feature_package"
)


def test_colp_single_feature_package():
    errors = validate.validate_package(COLP_PACKAGE_PATH)
    assert not errors
