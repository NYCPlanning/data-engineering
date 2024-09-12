import pandas as pd
from ingest.ingestion_helpers import read_from_excel

TEST_DATA_PATH = "resources/quality_of_life/diabetes_self_report/diabetes_self_report_processed_2023.xlsx"
TEST_SHEET_NAME = "DCHP_Diabetes_SelfRepHealth"
TEST_CATEGORY = "quality_of_life"


def test_read_excel_file():
    excel_file = read_from_excel(
        file_path=TEST_DATA_PATH,
        category=TEST_CATEGORY,
    )
    assert isinstance(excel_file, dict)
    assert TEST_SHEET_NAME in excel_file.keys()


def test_read_excel_sheet():
    excel_sheet = read_from_excel(
        file_path=TEST_DATA_PATH,
        category=TEST_CATEGORY,
        sheet_name="DCHP_Diabetes_SelfRepHealth",
    )
    assert isinstance(excel_sheet, pd.DataFrame)
    assert excel_sheet.shape == (65, 15)


def test_read_excel_columns():
    excel_sheet = read_from_excel(
        file_path=TEST_DATA_PATH,
        category=TEST_CATEGORY,
        sheet_name="DCHP_Diabetes_SelfRepHealth",
        columns="A:C, J:M",
    )
    assert isinstance(excel_sheet, pd.DataFrame)
    assert excel_sheet.shape == (65, 7)
