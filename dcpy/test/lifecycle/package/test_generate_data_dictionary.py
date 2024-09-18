import pytest
from dcpy.test.lifecycle.package.conftest import TEST_METADATA_YAML_PATH, TEMP_DATA_PATH

from dcpy.lifecycle.package import generate_metadata_assets

TEST_METADATA_HTML_PATH = TEMP_DATA_PATH / "metadata.html"
TEST_METADATA_PDF_PATH = TEMP_DATA_PATH / "metadata.pdf"


@pytest.mark.usefixtures("file_setup_teardown")
def test_generate_html_from_yaml():
    html_path = generate_metadata_assets.generate_html_from_yaml(
        yaml_file_path=TEST_METADATA_YAML_PATH,
        output_html_path=TEST_METADATA_HTML_PATH,
        html_template_path=generate_metadata_assets.DEFAULT_DATA_DICTIONARY_TEMPLATE_PATH,
    )
    assert html_path.exists()


@pytest.mark.usefixtures("file_setup_teardown")
def test_generate_pdf_from_html():
    html_path = generate_metadata_assets.generate_html_from_yaml(
        yaml_file_path=TEST_METADATA_YAML_PATH,
        output_html_path=TEST_METADATA_HTML_PATH,
        html_template_path=generate_metadata_assets.DEFAULT_DATA_DICTIONARY_TEMPLATE_PATH,
    )
    pdf_path = generate_metadata_assets.generate_pdf_from_html(
        output_html_path=html_path,
        output_pdf_path=TEST_METADATA_PDF_PATH,
    )
    assert pdf_path.exists()
