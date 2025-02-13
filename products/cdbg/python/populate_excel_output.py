import shutil
from pathlib import Path
from typing import TypedDict

from dcpy.utils import excel
from dcpy.utils.logging import logger

from . import PRODUCT_PATH, OUTPUT_PATH

TEMPLATE_EXCEL_PATH = PRODUCT_PATH / "census-tract-eligibility-template.xlsx"
OUTPUT_EXCEL_PATH = OUTPUT_PATH / "census-tract-eligibility.xlsx"


class CsvForExcel(TypedDict):
    csv_path: Path
    sheet_name: str
    row_offset: int | None


csvs_for_excel = [
    CsvForExcel(
        csv_path=OUTPUT_PATH / "cdbg_tracts_excel.csv",
        sheet_name="Tract",
        row_offset=5,
    ),
    CsvForExcel(
        csv_path=OUTPUT_PATH / "cdbg_boroughs_excel.csv",
        sheet_name="Borough",
        row_offset=6,
    ),
    CsvForExcel(
        csv_path=OUTPUT_PATH / "cdbg_block_groups_excel.csv",
        sheet_name="Block Group",
        row_offset=5,
    ),
]


def populate_excel_output():
    if OUTPUT_EXCEL_PATH.exists():
        Path.unlink(OUTPUT_EXCEL_PATH)

    shutil.copy(TEMPLATE_EXCEL_PATH, OUTPUT_EXCEL_PATH)

    logger.info(f"Populating excel file '{OUTPUT_EXCEL_PATH}' ...")
    for csv_for_excel in csvs_for_excel:
        logger.info(
            f"Populating sheet '{csv_for_excel['sheet_name']}' with csv '{csv_for_excel['csv_path']}' ..."
        )
        excel.csv_into_excel(
            csv_path=csv_for_excel["csv_path"],
            input_excel_path=OUTPUT_EXCEL_PATH,
            output_excel_path=OUTPUT_EXCEL_PATH,
            sheet_name=csv_for_excel["sheet_name"],
            row_ofset=csv_for_excel["row_offset"],
        )


if __name__ == "__main__":
    populate_excel_output()
