from pathlib import Path
import pytest
from dcpy.test.lifecycle.package.conftest import (
    PACKAGE_RESOURCES_PATH,
    TEST_ASSEMBLED_PACKAGE_AND_METADATA_PATH,
    TEST_METADATA_YAML_PATH,
    TEMP_DATA_PATH,
)

from dcpy.lifecycle.package import pdf_writer, xlsx_writer
from dcpy.models.product.metadata import OrgMetadata


@pytest.fixture
def org_metadata(resources_path: Path):
    return OrgMetadata.from_path(resources_path / "test_product_metadata_repo")


@pytest.mark.usefixtures("file_setup_teardown")
class TestDataDictionary(object):
    package_path = TEST_ASSEMBLED_PACKAGE_AND_METADATA_PATH
    yaml_path = TEST_METADATA_YAML_PATH
    html_path = TEMP_DATA_PATH / "metadata.html"
    output_pdf_path = TEMP_DATA_PATH / "metadata.pdf"
    output_xlsx_path = TEMP_DATA_PATH / "my_data_dictionary.xlsx"
    template_vars = {
        "heading": "Simple Heading",
        "sections": [
            {"heading": "Section 1", "content": "First section"},
            {"heading": "Section 2", "content": "Second section"},
        ],
    }

    def test_render_html_template(self):
        with open(PACKAGE_RESOURCES_PATH / "simple.html", "r") as f:
            expected = f.read()

        actual = pdf_writer._render_html_template(
            template_path=PACKAGE_RESOURCES_PATH / "simple.jinja",
            template_vars=self.template_vars,
        )
        assert actual == expected

    def test_render_html_template_document(self):
        with open(PACKAGE_RESOURCES_PATH / "document.html", "r") as f:
            expected = f.read()

        actual = pdf_writer._render_html_template(
            template_path=PACKAGE_RESOURCES_PATH / "document.jinja",
            template_vars=self.template_vars,
        )
        assert actual == expected

    def test_style_html(self):
        with open(PACKAGE_RESOURCES_PATH / "simple_styled.html", "r") as f:
            expected = f.read()

        with open(PACKAGE_RESOURCES_PATH / "simple.html", "r") as f:
            html = f.read()

        actual = pdf_writer._style_html(
            html=html,
            stylesheet_path=PACKAGE_RESOURCES_PATH / "simple_style.css",
        )
        assert actual == expected

    def test_style_html_document(self):
        with open(PACKAGE_RESOURCES_PATH / "document_styled.html", "r") as f:
            expected = f.read()

        with open(PACKAGE_RESOURCES_PATH / "document.html", "r") as f:
            html = f.read()

        actual = pdf_writer._style_html_document(
            html=html,
            stylesheet_path=PACKAGE_RESOURCES_PATH / "simple_style.css",
        )
        assert actual == expected

    def test_generate_pdf_from_yaml(self):
        pdf_writer.generate_html_from_yaml(
            metadata_path=self.yaml_path,
            output_path=self.html_path,
            html_template_path=pdf_writer.DEFAULT_DATA_DICTIONARY_TEMPLATE_PATH,
            stylesheet_path=pdf_writer.DEFAULT_DATA_DICTIONARY_STYLESHEET_PATH,
        )
        pdf_writer.generate_pdf_from_html(
            html_path=self.html_path,
            output_path=self.output_pdf_path,
            stylesheet_path=pdf_writer.DEFAULT_DATA_DICTIONARY_STYLESHEET_PATH,
        )
        assert self.output_pdf_path.exists()

    def test_generate_xslx(self, org_metadata):
        xlsx_writer.write_xlsx(
            org_md=org_metadata,
            product="transit_zones",  # This one has some mock revision history, so it's a good test case.
            output_path=TestDataDictionary.output_xlsx_path,
        )
        assert TestDataDictionary.output_xlsx_path.exists()
