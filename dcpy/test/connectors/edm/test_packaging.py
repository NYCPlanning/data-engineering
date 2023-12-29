import pytest
import os
from dcpy.connectors.edm import publishing, packaging


def test_package_fails(create_buckets, create_temp_filesystem, mock_data_constants):
    # packaging should fail when version is not published
    with pytest.raises(AssertionError, match=r"not found in S3 bucket"):
        packaging.package(
            publishing.PublishKey(
                mock_data_constants["TEST_PRODUCT_NAME"],
                mock_data_constants["TEST_VERSION"],
            )
        )


def test_package_stages(create_buckets, create_temp_filesystem, mock_data_constants):
    # sanity check that there are no packaged versions from previous tests or builds
    assert not packaging.get_packaged_versions(
        product=mock_data_constants["TEST_PRODUCT_NAME"]
    )

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
    publishing.download_published_version(publish_key, packaging.DOWNLOAD_ROOT_PATH)
    mock_data_constants["TEST_PACKAGE_METADATA"].packaging_function(
        publish_key, package_key
    )
    packaging.upload(package_key)

    # check published versions
    assert packaging.get_packaged_versions(
        mock_data_constants["TEST_PACKAGE_METADATA"].packaged_name
    ) == [mock_data_constants["TEST_VERSION"]]

    packaged_data_path = packaging.OUTPUT_ROOT_PATH / package_key.path
    packaging.download_packaged_version(package_key)
    filenames = [f for f in os.listdir(packaged_data_path)]

    # check published files
    assert mock_data_constants["TEST_PACKAGED_FILE"] in filenames
