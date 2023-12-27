import pytest
import os
from dcpy.connectors.edm import publishing, packaging


def test_bucket_empty(create_buckets, mock_data_constants):
    """Sanity check that there are no packaged versions from previous tests
    or actual data."""

    assert (
        packaging.get_packaged_versions(
            product=mock_data_constants["TEST_PRODUCT_NAME"]
        )
        == []
    )


def test_package_fails(create_buckets, create_temp_filesystem, mock_data_constants):
    # packaging should fail when version is not published
    with pytest.raises(AssertionError):
        packaging.package(
            publishing.PublishKey(
                mock_data_constants["TEST_PRODUCT_NAME"],
                mock_data_constants["TEST_VERSION"],
            )
        )


def test_package_stages(create_buckets, create_temp_filesystem, mock_data_constants):
    # draft and publish mock data
    draft_key = publishing.DraftKey(
        product=mock_data_constants["TEST_PRODUCT_NAME"],
        build=mock_data_constants["TEST_BUILD"],
    )
    build_data_path = mock_data_constants["TEST_DATA_DIR"]
    publishing.upload(
        output_path=build_data_path, draft_key=draft_key, acl=packaging.BUCKET_ACL
    )
    publishing.publish(
        draft_key=draft_key, acl=packaging.BUCKET_ACL, version=None, keep_draft=False
    )

    # package mock data
    publish_key = publishing.PublishKey(
        product=mock_data_constants["TEST_PRODUCT_NAME"],
        version=mock_data_constants["TEST_VERSION"],
    )
    package_key = packaging.PackageKey(
        product=mock_data_constants["TEST_PACKAGE_METADATA"].packaged_name,
        version=mock_data_constants["TEST_VERSION"],
    )

    packaging.download_published_version(publish_key)
    packaging.transform_for_packaging(
        publish_key,
        package_key,
        mock_data_constants["TEST_PACKAGE_METADATA"].packaging_function,
    )
    packaging.upload(package_key)

    assert packaging.get_packaged_versions(
        mock_data_constants["TEST_PACKAGE_METADATA"].packaged_name
    ) == [mock_data_constants["TEST_VERSION"]]

    packaged_data_path = packaging.OUTPUT_ROOT_PATH / package_key.path
    packaging.download_packaged_version(package_key)
    filenames = [f for f in os.listdir(packaged_data_path)]

    assert mock_data_constants["TEST_PACKAGED_FILE"] in filenames
