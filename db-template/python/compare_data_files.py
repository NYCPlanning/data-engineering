# Compare two data files
import pandas as pd

from python.utils import load_data_file

DATA_DIRECTORY = ".data/compare_jsons"
OLD_DATA_PATH = f"{DATA_DIRECTORY}/pff_metadata_2020.json"
NEW_DATA_PATH = f"{DATA_DIRECTORY}/pff_metadata_2021.json"
OUTPUT_FILE_PATH = f"{DATA_DIRECTORY}/compare_result.csv"

MAX_COMPARISON_ROWS = 100
NORMALIZE_VALUES = True

INDEX_COLUMN = "pff_variable"


def compare_data(old_data: pd.DataFrame, new_data: pd.DataFrame) -> None:
    print("PREVIEW OLD DATA")
    print(old_data.head().to_markdown())
    old_data.info()
    print("PREVIEW NEW DATA")
    new_data.info()
    print(new_data.head().to_markdown())

    compare_result = old_data.compare(new_data, align_axis=0, keep_equal=True)
    rows_with_diff_count = len(compare_result) // 2

    if rows_with_diff_count == 0:
        print(f"Files are identical ({len(old_data)} rows)")
    else:
        print(
            f"""
            Files aren't identical in
                {rows_with_diff_count:,} rows out of
                {len(old_data):,} rows in old data
                {len(new_data):,} rows in new data
            """
        )
        compare_result = compare_result.set_index(
            compare_result.index.set_levels(["old_data", "new_data"], level=1)
        )
        print("exporting comparison results to csv\n")
        compare_result.to_csv(OUTPUT_FILE_PATH)
        print(f"showing first {MAX_COMPARISON_ROWS} rows of comparison results\n")
        print(compare_result.head(MAX_COMPARISON_ROWS))


def limit_rows_to_compare_simple(
    data: pd.DataFrame, index_column: str, common_indices: set
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return data[data[index_column].isin(common_indices)].reset_index(drop=True)


def limit_rows_to_compare(
    old_data: pd.DataFrame,
    new_data: pd.DataFrame,
    index_column: str,
    common_indices: set,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    columns_to_use = old_data.columns
    if len(old_data.columns) > len(new_data.columns):
        print("!! old data has more columns")
        columns_to_use = new_data.columns
    elif len(new_data.columns) > len(old_data.columns):
        print("!! new data has more columns")
        columns_to_use = old_data.columns

    old_data = old_data[columns_to_use]
    new_data = new_data[columns_to_use]
    columns_to_sort_by = columns_to_use.to_list()

    old_data_limited = (
        old_data[old_data[index_column].isin(common_indices)]
        .sort_values(by=columns_to_sort_by)
        .reset_index(drop=True)
    )
    new_data_limited = (
        new_data[new_data[index_column].isin(common_indices)]
        .sort_values(by=columns_to_sort_by)
        .reset_index(drop=True)
    )

    return old_data_limited, new_data_limited


def show_data_with_unique_indices(
    old_data: pd.DataFrame,
    new_data: pd.DataFrame,
    index_column: str,
) -> None:
    unique_indices_from_old = set(old_data[index_column].unique()).difference(
        set(new_data[index_column].unique())
    )
    print("Unique rows in OLD DATA")
    print(old_data[old_data[index_column].isin(unique_indices_from_old)].to_markdown())
    unique_indices_from_new = set(new_data[index_column].unique()).difference(
        set(old_data[index_column].unique())
    )
    print("Unique rows in NEW DATA")
    print(new_data[new_data[index_column].isin(unique_indices_from_new)].to_markdown())


def normalize_data(data: pd.DataFrame) -> pd.DataFrame:
    data = data.infer_objects()
    for column in data.columns:
        column_type = data[column].dtype
        if isinstance(column_type, object) and isinstance(data[column].iloc[0], str):
            print(f"Normalized string column {column}")
            data[column] = data[column].str.lower()
        elif isinstance(column_type, object) and isinstance(data[column].iloc[0], list):
            data[column] = data[column].apply(tuple)
            print(f"Normalized list column {column}")

    return data


if __name__ == "__main__":
    print("loading data ...")
    pd.set_option("display.max_rows", MAX_COMPARISON_ROWS)
    old_data_raw = load_data_file(filepath=OLD_DATA_PATH)
    old_data_raw.columns = old_data_raw.columns.str.lower()
    new_data_raw = load_data_file(filepath=NEW_DATA_PATH)
    new_data_raw.columns = new_data_raw.columns.str.lower()

    # confirm chosen index column is unique in both datasets
    for data in [old_data_raw, new_data_raw]:
        if len(data[INDEX_COLUMN].unique()) != len(data):
            raise ValueError(f"Column {INDEX_COLUMN} is not unique in a dataset!")

    if NORMALIZE_VALUES:
        print("normalizing data ...")
        old_data_raw = normalize_data(old_data_raw)
        new_data_raw = normalize_data(new_data_raw)

    print("comparing ...")
    if len(old_data_raw) == len(new_data_raw):
        compare_data(old_data=old_data_raw, new_data=new_data_raw)
    else:
        print("WARNING! Can only compare data of the same length and indices!")
        print(
            f"""
                {len(old_data_raw):,} rows in old data
                {len(new_data_raw):,} rows in new data
            """
        )
        print("detail differences ...")
        show_data_with_unique_indices(old_data_raw, new_data_raw, INDEX_COLUMN)
        common_indices = set(old_data_raw[INDEX_COLUMN].unique()).intersection(
            set(new_data_raw[INDEX_COLUMN].unique())
        )

        print("compare common rows ...")
        print(f"{len(common_indices)} common index values")
        old_data_to_compare, new_data_to_compare = limit_rows_to_compare(
            old_data_raw, new_data_raw, INDEX_COLUMN, common_indices
        )
        compare_data(old_data_to_compare, new_data_to_compare)
