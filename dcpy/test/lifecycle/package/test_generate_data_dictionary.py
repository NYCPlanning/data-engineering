import pytest
from unittest import TestCase
from dcpy.test.lifecycle.package.conftest import (
    TEST_ASSEMBLED_PACKAGE_AND_METADATA_PATH,
    TEST_METADATA_YAML_PATH,
    TEMP_DATA_PATH,
)

from dcpy.lifecycle.package import generate_metadata_assets, oti_xlsx


@pytest.mark.usefixtures("file_setup_teardown")
class TestDataDictionary(TestCase):
    package_path = TEST_ASSEMBLED_PACKAGE_AND_METADATA_PATH
    yaml_file_path = TEST_METADATA_YAML_PATH
    output_html_path = TEMP_DATA_PATH / "metadata.html"
    output_pdf_path = TEMP_DATA_PATH / "metadata.pdf"
    output_xlsx_path = TEMP_DATA_PATH / "my_data_dictionary.xlsx"

    def test_generate_html_from_yaml(self):
        html_path = generate_metadata_assets.generate_html_from_yaml(
            yaml_file_path=self.yaml_file_path,
            output_html_path=self.output_html_path,
            html_template_path=generate_metadata_assets.DEFAULT_DATA_DICTIONARY_TEMPLATE_PATH,
        )
        assert html_path.exists()

    def test_generate_pdf_from_html(self):
        html_path = generate_metadata_assets.generate_html_from_yaml(
            yaml_file_path=self.yaml_file_path,
            output_html_path=self.output_html_path,
            html_template_path=generate_metadata_assets.DEFAULT_DATA_DICTIONARY_TEMPLATE_PATH,
        )
        pdf_path = generate_metadata_assets.generate_pdf_from_html(
            output_html_path=html_path,
            output_pdf_path=self.output_pdf_path,
        )
        assert pdf_path.exists()

    def test_generate_xslx(self):
        oti_xlsx.write_oti_xlsx(
            metadata_path=self.package_path / "metadata.yml",
            output_path=self.output_xlsx_path,
        )
        assert self.output_xlsx_path.exists()
