import pandas as pd

from dcpy.utils.json import df_to_json
from pipelines.utils import parse_args, download_manual_update, s3_upload, DATA_PATH


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

PIVOT_COLUMNS = ["year", "geoid"]

COLUMN_CLEANUP = {"Male ": "Male", "Male P": "MaleP"}

OUTPUT_FOLDER = DATA_PATH / ".output" / "decennial"


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    # clean community districts
    df["geoid"] = df.apply(
        lambda x: "CCD" + x["geoid"] if x["geogtype"] == "CCD2023" else x["geoid"],
        axis=1,
    )
    df.drop("geogtype", axis=1, inplace=True)
    return df


def process_data_sheet(excel_file, year, sheet_name):
    print(f"Processing data for decennial year {year} using sheet '{sheet_name}'")
    output_file = OUTPUT_FOLDER / year / "decennial.csv"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    df.rename(columns=COLUMN_CLEANUP, inplace=True)
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
    df = clean_data(df)
    df = df.melt(id_vars=PIVOT_COLUMNS)

    print("Exporting")
    df.to_csv(output_file, index=False)
    return output_file


def process_metadata(excel_file):
    df = pd.read_excel(excel_file, sheet_name="Data Dictionary", skiprows=3)
    df = df.dropna(subset=["Category"])
    df["VariableName"] = df["VariableName"].apply(lambda c: COLUMN_CLEANUP.get(c, c))
    df["year"] = df["Dataset"].astype(str).str.split(", ")
    df = df.explode("year")
    columns = {
        "VariableName": "pff_variable",
        "Relation": "base_variable",
        "Category": "category",
    }
    df.rename(columns=columns, inplace=True)
    files = {}
    for year, year_df in df.groupby("year"):
        year_df = year_df[columns.values()]
        year_df["domain"] = "decennial"
        file = OUTPUT_FOLDER / year / "metadata.json"
        file.parent.mkdir(parents=True, exist_ok=True)
        with open(file, "w") as outfile:
            outfile.write(df_to_json(year_df))
        files[year] = file
    return files


if __name__ == "__main__":
    input_year, geography, upload = parse_args()
    print("read in excel ...")
    input_file = download_manual_update("decennial", "latest")

    excel = pd.ExcelFile(input_file)

    metadata_files = process_metadata(excel)

    for _, year in YEAR_CONFIG.iterrows():
        if not input_year or (input_year == year["year"]):
            output_file = process_data_sheet(excel, year["year"], year["sheet_name"])
            print(f"Finished decennial year {year['year']}")

            if upload:
                print(f"Uploading {output_file}")
                s3_upload(output_file)
                if year["year"] in metadata_files:
                    s3_upload(metadata_files[year["year"]])
