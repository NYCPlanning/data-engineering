import pytest
from unittest import TestCase
from dcpy.test.lifecycle.package.conftest import (
    PACKAGE_RESOURCES_PATH,
    TEST_ASSEMBLED_PACKAGE_AND_METADATA_PATH,
    TEST_METADATA_YAML_PATH,
    TEMP_DATA_PATH,
)

from dcpy.lifecycle.package import generate_metadata_assets, oti_xlsx
from dcpy.models.product.dataset import metadata_v2 as md


@pytest.mark.usefixtures("file_setup_teardown")
class TestDataDictionary(TestCase):
    package_path = TEST_ASSEMBLED_PACKAGE_AND_METADATA_PATH
    yaml_path = TEST_METADATA_YAML_PATH
    html_path = TEMP_DATA_PATH / "metadata.html"
    pdf_path = TEMP_DATA_PATH / "metadata.pdf"
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

        actual = generate_metadata_assets._render_html_template(
            template_path=PACKAGE_RESOURCES_PATH / "simple.jinja",
            template_vars=self.template_vars,
        )
        assert actual == expected

    def test_render_html_template_document(self):
        with open(PACKAGE_RESOURCES_PATH / "document.html", "r") as f:
            expected = f.read()

        actual = generate_metadata_assets._render_html_template(
            template_path=PACKAGE_RESOURCES_PATH / "document.jinja",
            template_vars=self.template_vars,
        )
        assert actual == expected

    def test_style_html(self):
        with open(PACKAGE_RESOURCES_PATH / "simple_styled.html", "r") as f:
            expected = f.read()

        with open(PACKAGE_RESOURCES_PATH / "simple.html", "r") as f:
            html = f.read()

        actual = generate_metadata_assets._style_html(
            html=html,
            stylesheet_path=PACKAGE_RESOURCES_PATH / "simple_style.css",
        )
        assert actual == expected

    def test_style_html_document(self):
        with open(PACKAGE_RESOURCES_PATH / "document_styled.html", "r") as f:
            expected = f.read()

        with open(PACKAGE_RESOURCES_PATH / "document.html", "r") as f:
            html = f.read()

        actual = generate_metadata_assets._style_html_document(
            html=html,
            stylesheet_path=PACKAGE_RESOURCES_PATH / "simple_style.css",
        )
        assert actual == expected

    def test_generate_pdf_from_yaml(self):
        html_path = generate_metadata_assets.generate_html_from_yaml(
            metadata_path=self.yaml_path,
            output_path=self.html_path,
            html_template_path=generate_metadata_assets.DEFAULT_DATA_DICTIONARY_TEMPLATE_PATH,
        )
        pdf_path = generate_metadata_assets.generate_pdf_from_html(
            html_path=html_path,
            output_path=self.pdf_path,
            stylesheet_path=generate_metadata_assets.DEFAULT_DATA_DICTIONARY_STYLESHEET_PATH,
        )
        assert pdf_path.exists()

    def test_generate_xslx(self):
        oti_xlsx.write_oti_xlsx(
            dataset=md.Metadata.from_path(self.package_path / "metadata.yml").dataset,
            output_path=self.output_xlsx_path,
        )
        assert self.output_xlsx_path.exists()
