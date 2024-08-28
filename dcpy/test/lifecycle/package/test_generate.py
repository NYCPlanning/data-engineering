from pathlib import Path
from dcpy.lifecycle.package import generate_metadata_assets

TEST_PACKAGE_PATH = Path(__file__).parent.resolve() / "resources" / "test_package"
TEST_METADATA_YAML_PATH = TEST_PACKAGE_PATH / "metadata_v2.yml"
TEST_METADATA_HTML_PATH = TEST_PACKAGE_PATH / "output" / "metadata.html"
TEST_METADATA_PDF_PATH = TEST_PACKAGE_PATH / "output" / "metadata.pdf"


def test_generate_html_from_yaml():
    html_path = generate_metadata_assets.generate_html_from_yaml(
        yaml_file_path=TEST_METADATA_YAML_PATH,
        output_html_path=TEST_METADATA_HTML_PATH,
        html_template_path=generate_metadata_assets.DEFAULT_DATA_DICTIONARY_TEMPLATE_PATH,
    )
    assert html_path.exists()


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
