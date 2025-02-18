import pytest
from dcpy.test.lifecycle.package.conftest import (
    PACKAGE_RESOURCES_PATH,
    TEMP_DATA_PATH,
)

from dcpy.lifecycle.package import yaml_writer, pdf_writer, xlsx_writer


@pytest.fixture
def org_metadata(package_and_dist_test_resources):
    return package_and_dist_test_resources.org_md()


@pytest.mark.usefixtures("file_setup_teardown")
class TestDataDictionary(object):
    product = "transit_zones"  # This one has some mock revision history, so it's a good test case.
    output_yml_path = TEMP_DATA_PATH / "my_data_dictionary.yml"
    output_pdf_path = TEMP_DATA_PATH / "my_data_dictionary.pdf"
    output_xlsx_path = TEMP_DATA_PATH / "my_data_dictionary.xlsx"

    def test_generate_yml(self, org_metadata):
        yaml_writer.write_yaml(
            org_metadata=org_metadata,
            product=self.product,
            output_path=TestDataDictionary.output_yml_path,
        )
        assert TestDataDictionary.output_yml_path.exists()

    def test_generate_pdf(self, org_metadata):
        pdf_writer.write_pdf(
            org_metadata=org_metadata,
            product=self.product,
            output_path=TestDataDictionary.output_pdf_path,
        )
        assert TestDataDictionary.output_pdf_path.exists()

    def test_generate_xslx(self, org_metadata):
        xlsx_writer.write_xlsx(
            org_md=org_metadata,
            product=self.product,
            output_path=TestDataDictionary.output_xlsx_path,
        )
        assert TestDataDictionary.output_xlsx_path.exists()


@pytest.mark.usefixtures("file_setup_teardown")
class TestDataDictionaryDataset(object):
    product = "lion"
    dataset = "school_districts"

    output_yml_path = TEMP_DATA_PATH / "my_data_dictionary.yml"
    output_pdf_path = TEMP_DATA_PATH / "my_data_dictionary.pdf"
    output_xlsx_path = TEMP_DATA_PATH / "my_data_dictionary.xlsx"

    def test_generate_yml(self, org_metadata):
        yaml_writer.write_yaml(
            org_metadata=org_metadata,
            product=self.product,
            dataset=self.dataset,
            output_path=TestDataDictionary.output_yml_path,
        )
        assert TestDataDictionary.output_yml_path.exists()

        yaml_writer.write_yaml(
            org_metadata=org_metadata,
            product=self.product,
            dataset=self.dataset,
            output_path=TestDataDictionary.output_yml_path,
        )
        assert TestDataDictionary.output_yml_path.exists()

    def test_generate_pdf(self, org_metadata):
        pdf_writer.write_pdf(
            org_metadata=org_metadata,
            product=self.product,
            dataset=self.dataset,
            output_path=TestDataDictionary.output_pdf_path,
        )
        assert TestDataDictionary.output_pdf_path.exists()

    def test_generate_xslx(self, org_metadata):
        xlsx_writer.write_xlsx(
            org_md=org_metadata,
            product=self.product,
            dataset=self.dataset,
            output_path=TestDataDictionary.output_xlsx_path,
        )
        assert TestDataDictionary.output_xlsx_path.exists()


@pytest.mark.usefixtures("file_setup_teardown")
class TestHTMLRender(object):
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
