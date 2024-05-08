from pathlib import Path
from dcpy.lifecycle.package import validate
from dcpy.models.product.dataset.metadata import Metadata as md

COLP_PACKAGE_PATH = (
    Path(__file__).parent.resolve() / "resources" / "colp_single_feature_package"
)


def test_colp_single_feature_package():
    validation = validate.validate_package(COLP_PACKAGE_PATH)
    assert len(validation.validations) == 2
    assert not validation.get_dataset_errors()


def test_missing_attachments():
    overridden_md = md.from_yaml(COLP_PACKAGE_PATH / "metadata.yml")

    fake_attachment_name = "I_dont_exist.pdf"
    overridden_md.package.attachments.append(fake_attachment_name)

    validation = validate.validate_package(COLP_PACKAGE_PATH, overridden_md)
    assert (
        len(validation.errors) == 1
    ), "An error should have been found for the missing attachment"

    assert fake_attachment_name in validation.errors[0].message
