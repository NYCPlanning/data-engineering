# This script transforms and uploads ACS data provided by the NYCDCP Population team
from pathlib import Path
import re
import pandas as pd
from pipelines import PRODUCT_PATH
from pipelines.utils import parse_args, download_manual_update, s3_upload


YEAR_CONFIG = pd.DataFrame(
    [
        {
            "year": "2010",
            "geog": "2010_to_2020",
            "sheet_name": "2010 (in 2020 Geogs)",
        },
        {
            "year": "2020",
            "geog": "2010_to_2020",
            "sheet_name": "2020",
        },
    ]
)

## columns in manual xlsx provided were in output schema as defined in factfinder/data/decennial metadata files
## however, the following columns were inconsistent. This maps input to desired output column
## this may not remain stable year-to-year
COLUMN_MAPPING = {
    "popacre": "popperacre",
    "popacrep": "popperacrep",
    "popinhh_1": "popinhh",
    "popinhh_1p": "popinhhp",
    "popu18": "popu18_1",
    "popu18p": "popu18_1p",
}

PIVOT_COLUMNS = ["year", "geoid"]

## TODO hard-coded for now - need to think about how we want to specify this
INPUT_FILEPATH = (
    PRODUCT_PATH
    / "factfinder/data/decennial_manual_updates/2023/dcp_pop_decennial_dhc.xlsx"
)


## attempt to parse years from file. Seems likely fragile and more worth defining in constant, see YEAR_CONFIG. Leaving in case we return to it
def get_sheet_metadata(excel_file: pd.ExcelFile):
    sheet_metadata = {}
    for sheet_name in excel_file.sheet_names:
        regex = re.match("^(\d{4})(.*\(.*(\d{4}).*\))?", sheet_name)
        if regex:
            sheet_year = regex.group(1)
            geog_conversion = regex.group(3)
            if geog_conversion:
                geog = f"{regex.group(1)}_to_{regex.group(3)}"
            else:
                geog = sheet_year
            metadata = {"sheet_name": sheet_name, "geog": geog}
            sheet_metadata[sheet_year] = metadata
    return sheet_metadata


def clean_data(df: pd.DataFrame, year: str) -> pd.DataFrame:
    # clean community districts
    df["geoid"] = df.apply(
        lambda x: "CCD" + x["geoid"] if x["geogtype"] == "CCD2023" else x["geoid"],
        axis=1,
    )

    # filter columns by what's needed for output
    metadata_file = f"factfinder/data/decennial/{year}/metadata.json"
    output_columns = list(pd.read_json(metadata_file)["pff_variable"])
    output_columns = output_columns + [column + "p" for column in output_columns]
    df.columns = [COLUMN_MAPPING.get(column, column) for column in df.columns]
    df = df[
        [column for column in df.columns if column in (output_columns + PIVOT_COLUMNS)]
    ]
    return df


def process_excel_file(excel_file, year, sheet_name):
    print(f"Processing data for decennial year {year} using sheet '{sheet_name}'")
    output_file = Path(".output/decennial") / year / "decennial.csv"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    df.columns = df.columns.str.lower()
    df["geoid"] = df["geoid"].astype(str)

    ## export columns and unique geogtypes as a qc check
    pd.DataFrame({"geogtype": df["geogtype"].unique()}).to_csv(
        output_file.parent / "input_geogtypes.csv", index=False
    )
    pd.DataFrame({"ColumnName": df.columns}).to_csv(
        output_file.parent / "input_columns.csv", index=False
    )

    print("Cleaning data")
    df = clean_data(df, year)
    df = df.melt(id_vars=PIVOT_COLUMNS)

    print("Exporting")
    df.to_csv(output_file, index=False)
    return output_file


if __name__ == "__main__":
    input_year, geography, upload = parse_args()
    print("read in excel ...")
    input_file = download_manual_update("decennial", "latest")

    excel = pd.ExcelFile(input_file)

    for _, year in YEAR_CONFIG.iterrows():
        if not input_year or (input_year == year["year"]):
            output_file = process_excel_file(excel, year["year"], year["sheet_name"])
            print(f"Finished decennial year {year['year']}")

            output_file = Path(".output/decennial") / year["year"] / "decennial.csv"
            output_file.parent.mkdir(parents=True, exist_ok=True)

            if upload:
                print(f"Uploading {output_file}")
                s3_upload(output_file)
