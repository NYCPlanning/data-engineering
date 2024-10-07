import openpyxl  # type: ignore
from openpyxl.styles import Border, Side, Alignment, Font  # type: ignore
from pathlib import Path
from tabulate import tabulate  # type: ignore
import typer


from dcpy.models.product.dataset import metadata_v2 as md_v2
from dcpy.utils.logging import logger

from . import RESOURCES_PATH

DEFAULT_TEMPLATE_PATH = RESOURCES_PATH / "oti_data_dictionary_template.xlsx"


OTI_METADATA_FILE_TYPE = "oti_data_dictionary"


class OTI_XLSX_TABS:
    dataset_info = "Dataset Information"
    column_information = "Column Information"
    revision_history_information = "Dataset Revision History"


DISCLAIMER = """\
  This dataset is being provided by the Department of City Planning (DCP) on DCP’s\
  website for informational purposes only. DCP does not warranty the completeness,\
  accuracy, content, or fitness for any particular purpose or use of the dataset,\
  nor are any such warranties to be implied or inferred with respect to the dataset\
  as furnished on the website. DCP and the City are not liable for any deficiencies\
  in the completeness, accuracy, content, or fitness for any particular purpose or\
  use the dataset, or applications utilizing the dataset, provided by any third party.\
"""

# pulling this out solely for a test case.
_DESCRIPTION_ROW_INDEX = 15


def _get_dataset_description(path: Path):
    xlsx_wb = openpyxl.load_workbook(filename=path)
    ds_info = xlsx_wb[OTI_XLSX_TABS.dataset_info]

    rows = [r for r in ds_info.rows]
    return rows[_DESCRIPTION_ROW_INDEX][1].value


def _write_dataset_information(xlsx_wb: openpyxl.Workbook, metadata: md_v2.Dataset):
    ds_info_sheet = xlsx_wb[OTI_XLSX_TABS.dataset_info]

    rows = [r for r in ds_info_sheet.rows]

    # Dataset Name: 8
    rows[8][1].value = metadata.attributes.display_name

    # Dataset URL: 9
    # TODO

    # The name of the NYC agency providing this data to the public.": 10
    rows[10][1].value = metadata.attributes.display_name

    # Each Row Is A: The unit of analysis/level of aggregation of the dataset": 11
    rows[11][1].value = metadata.attributes.each_row_is_a

    # Publishing Frequency. How often changed data is published to this dataset. For an automatically updated dataset, this is the frequency of that automation": 12
    rows[12][1].value = metadata.attributes.publishing_frequency

    # How often the data underlying this dataset is changed": 13
    rows[13][1].value = metadata.attributes.publishing_frequency

    # Frequency Details. Additional details about the publishing or data change frequency, if needed": 14
    rows[14][1].value = metadata.attributes.publishing_frequency_details

    # Dataset Description. Overview of the information this dataset contains, including overall context and definitions of key terms. This field may include links to supporting datasets, agency websites, or external resources for additional context. ": 15
    rows[_DESCRIPTION_ROW_INDEX][1].value = metadata.attributes.description

    # Why is this dataset collected. Purpose behind the collection of this data, including any legal or policy requirements for this data by NYC Executive Order, Local Law, or other policy directive.": 16
    rows[16][1].value = metadata.attributes.publishing_purpose

    # How is this data collectred? If data collection includes fielding applications, requests, or complaints, this field includes details about the forms, applications, and processes used.  ": 17
    rows[17][1].value = metadata.attributes.publishing_purpose

    # "How can this data be used? What are some questions one might answer using this dataset?": 18
    rows[18][1].value = metadata.attributes.potential_uses

    # What are the unique characteristics or limitations of this dataset? Unique characteristics of this dataset to be aware of, specifically, constraints or limitations to the use of the data.": 19
    rows[19][1].value = DISCLAIMER
    # For any datasets with geospatial data, specify the coordinate reference system or projection used and other relevant details.": 20
    rows[20][1].value = metadata.attributes.projection


def _set_default_style(cell, *, is_rightmost=True, is_last_row=False):
    border_side_thin = Side(border_style="thin", color="000000")
    border_side_medium = Side(border_style="medium", color="000000")

    cell.alignment = Alignment(wrapText=True, vertical="top")
    cell.border = Border(
        top=border_side_thin,
        left=border_side_thin,
        right=border_side_medium if is_rightmost else border_side_thin,
        bottom=border_side_medium if is_last_row else border_side_thin,
    )


def _format_row_slice(row_slice, is_last_row=False):
    """Format a row slice with OTI's table formatting."""
    *left_cells, rightmost_cell = row_slice
    [
        _set_default_style(
            c,
            is_last_row=is_last_row,
            is_rightmost=False,
        )
        for c in left_cells
    ]
    _set_default_style(rightmost_cell, is_last_row=is_last_row, is_rightmost=True)


def _write_column_information(xlsx_wb: openpyxl.Workbook, metadata: md_v2.Dataset):
    ds_info_sheet = xlsx_wb[OTI_XLSX_TABS.column_information]

    header_description_row_index = 2
    ds_info_sheet.insert_rows(header_description_row_index + 2, len(metadata.columns))

    rows = [r for r in ds_info_sheet.rows]

    for idx, md_col in enumerate(metadata.columns):
        new_row_idx = idx + header_description_row_index + 1

        row_slice = rows[new_row_idx][0:5]
        col_name, col_val, col_expected_values, col_field_limits, col_notes = row_slice

        # The basics
        col_name.value = md_col.name
        col_val.value = md_col.description
        col_field_limits.value = ""  # TODO: field limitations
        col_notes.value = md_col.notes

        # Standardized Values Table
        col_expected_values.font = Font(
            name="Consolas"
        )  # Need a monospaced font for tables columns to align
        col_expected_values.value = (
            tabulate(
                [
                    [str(v.value) + " ", str(v.description or " ") + " "]  # bool issue
                    for v in md_col.values
                ],
                headers=["Value", "Description"],
                tablefmt="presto",
                maxcolwidths=[10, 40],
            )
            if md_col.values
            else ""
        )

        _format_row_slice(row_slice, is_last_row=idx == (len(metadata.columns) - 1))


def _write_change_history(xlsx_wb: openpyxl.Workbook, change_log: list[list[str]]):
    rev_sheet = xlsx_wb[OTI_XLSX_TABS.revision_history_information]
    header_description_row_index = 2

    rev_sheet.insert_rows(header_description_row_index + 2, len(change_log))

    rows = [r for r in rev_sheet.rows]
    for idx, rev in enumerate(change_log):
        row_slice = rows[idx + header_description_row_index + 1][0:3]
        col_date, col_change_highlights, col_comments = row_slice

        col_date.value = rev[0]
        col_change_highlights.value = rev[1]
        col_comments.value = rev[2]

        _format_row_slice(row_slice, is_last_row=idx == (len(change_log) - 1))


def write_oti_xlsx(
    *,
    dataset: md_v2.Dataset,
    output_path: Path | None = None,
    template_path_override: Path | None = None,
):
    xlsx_wb = openpyxl.load_workbook(
        filename=template_path_override or DEFAULT_TEMPLATE_PATH
    )
    _write_dataset_information(xlsx_wb, dataset)
    _write_column_information(xlsx_wb, dataset)
    # TODO: this locationw will change
    _write_change_history(xlsx_wb, dataset.attributes.custom.get("change_log", []))

    out_path = output_path or Path("./data_dictionary.xlsx")
    logger.info(f"Saving OTI XLSX to {out_path}")
    xlsx_wb.save(out_path)


app = typer.Typer()


@app.command("generate_xlsx")
def _write_oti_xlsx_cli(
    metadata_path: Path,
    output_path: Path = typer.Option(
        None,
        "--output-path",
        "-o",
        help="Output Path. Defaults to ./data_dictionary.xlsx",
    ),
    template_path_override: Path = typer.Option(
        None,
        "--template-path",
        "-t",
        help="(Override) Template Path",
    ),
):
    write_oti_xlsx(
        dataset=md_v2.Metadata.from_path(metadata_path).dataset,
        output_path=output_path,
        template_path_override=template_path_override,
    )
