from faker import Faker
from pandas import DataFrame as df
from pathlib import Path
import random
from shapely import wkb, wkt
import uuid

import dcpy.models.product_metadata as models
from dcpy.models.product_metadata import Metadata as md
from dcpy.lifecycle.package import validate

METADATA_PATH = (
    Path(__file__).parent.resolve() / "resources" / "test_package" / "metadata.yml"
)
print(METADATA_PATH)
assert METADATA_PATH.exists()

rd = random.Random()
rd.seed(0)
faker = Faker()

metadata = md.from_yaml(METADATA_PATH)


class DCPFakes:
    @staticmethod
    def wkt():
        return wkt.loads(f"Point({faker.latitude()} {faker.longitude()})")

    @staticmethod
    def wkb():
        return wkb.dumps(DCPFakes.wkt(), hex=True, srid=4326)

    @staticmethod
    def boro_code():
        return str(random.randrange(1, 6))

    @staticmethod
    def block():
        return str(random.randrange(1, 100 * 1000))  # .zfill(5)

    @staticmethod
    def lot():
        return str(random.randrange(1, 1000))

    @staticmethod
    def bbl(boro_code, block, lot):
        return f"{boro_code}{block.zfill(5)}{lot.zfill(4)}"


fakes = {
    "wkt": DCPFakes.wkt,
    "wkb": DCPFakes.wkb,
    "boro_code": DCPFakes.boro_code,
    "block": DCPFakes.block,
    "lot": DCPFakes.lot,
    "bbl": DCPFakes.bbl,
    "uid": lambda: str(uuid.UUID(int=rd.getrandbits(128), version=4)),
    "text": faker.pystr,
    "latitude": lambda: str(faker.latitude()),
    "longitude": lambda: str(faker.longitude()),
}


def _fake_row(columns: list[models.Column]):
    row = {}

    found_bbl_parts = {}
    bbl_parts = {"boro_code", "block", "lot"}
    found_bbl_name = ""
    for c in columns:
        if c.data_type == "bbl":
            found_bbl_name = c.name
        elif c.values:
            row[c.name] = random.choice(c.values)[0]
        else:
            val = fakes[c.data_type]()
            row[c.name] = val
            if c.data_type in {"boro_code", "block", "lot"}:
                found_bbl_parts[c.data_type] = val

    # Construct a BBL from found parts, or generate a new one
    if found_bbl_name:
        if set(found_bbl_parts.keys()) == bbl_parts:
            row[found_bbl_name] = fakes["bbl"](
                found_bbl_parts["boro_code"],
                found_bbl_parts["block"],
                found_bbl_parts["lot"],
            )
        else:
            row[found_bbl_name] = fakes["bbl"](
                fakes["boro_code"](),
                fakes["block"](),
                fakes["lot"](),
            )

    for c in columns:
        if not c.non_nullable and random.choice([True, False]):
            # adding some extra chaos
            if random.choice([True, False]):
                del row[c.name]
            else:
                row[c.name] = ""
    return row


def generate_fake_dataset(row_count: int, columns: list[models.Column]):
    return df.from_records([_fake_row(columns) for i in range(row_count)])


# AR note: I mostly use this to conveniently get a fake dataset
# in a Jupyter Notebook.
def _generate_fake_dataset_from_test_md(row_count: int):
    dataset = metadata.package.get_dataset("primary_csv")
    return generate_fake_dataset(row_count, columns=dataset.get_columns(metadata))


def test_socrata_column_overrides_wkb_geom():
    dataset = metadata.package.get_dataset("primary_shapefile")
    col_names = {c.name for c in dataset.get_columns(metadata)}

    assert (
        "the_geom" in col_names
    ), """expected a column named `the_geom`.
    It should have overridden the existing column named wkb_geometry"""

    # Shapefile
    socrata_dest_cols_shp = metadata.get_destination(
        "socrata_shapefile"
    ).destination_column_metadata(metadata)
    socrata_dest_col_names_shp = {c.api_name for c in socrata_dest_cols_shp}
    assert (
        "the_geom_socrata" in socrata_dest_col_names_shp
    ), "The shapefile name should have been transformed to the socrata destination name"

    assert "omit_me_from_shapefile" not in socrata_dest_col_names_shp

    OVERRIDDEN_DISPLAY_COL = "the_geom"
    the_geom_field = [
        c for c in socrata_dest_cols_shp if c.name == OVERRIDDEN_DISPLAY_COL
    ][0]

    assert the_geom_field.display_name == "the_geom_socrata_overridden_display"

    # CSV
    socrata_dest_cols_csv = metadata.get_destination(
        "socrata_csv"
    ).destination_column_metadata(metadata)
    socrata_dest_col_names_csv = {c.api_name for c in socrata_dest_cols_csv}

    assert "omit_me_from_shapefile" in socrata_dest_col_names_csv


def test_validating_valid_data():
    dataset = metadata.package.get_dataset("primary_csv")
    fake_ds = generate_fake_dataset(100, columns=dataset.get_columns(metadata))
    results = validate.validate_df(fake_ds, dataset, metadata)

    assert results == [], "No Errors should have been found in source data"


def test_invalid_standardized_values():
    dataset = metadata.package.get_dataset("primary_csv")
    ROW_COUNT = 100
    fake_ds = generate_fake_dataset(ROW_COUNT, columns=dataset.get_columns(metadata))

    INVALID_OWNERSHIP_VALUES = ["X", "Y", "Z"]
    fake_ds.loc[0, "non_nullable_ownership"] = INVALID_OWNERSHIP_VALUES[0]
    fake_ds.loc[1, "non_nullable_ownership"] = INVALID_OWNERSHIP_VALUES[1]
    fake_ds.loc[2:, "non_nullable_ownership"] = INVALID_OWNERSHIP_VALUES[2]

    results = validate.validate_df(fake_ds, dataset, metadata)
    assert (
        len(results) == 1
    ), "One error should have been found for invalid standardized values"

    # Assert that the error message should mention the invalid values with their counts
    result_msg = results[0][1]
    assert (
        f"'{INVALID_OWNERSHIP_VALUES[0]}': 1," in result_msg
    ), "The error message should include the invalid value and count"
    assert (
        f"'{INVALID_OWNERSHIP_VALUES[1]}': 1," in result_msg
    ), "The error message should include the invalid value and count"
    assert (
        f"'{INVALID_OWNERSHIP_VALUES[2]}': {ROW_COUNT - 2}" in result_msg
    ), "The error message should include the invalid value and count"


def test_standardized_values_with_nulls():
    dataset = metadata.package.get_dataset("primary_csv")
    ROW_COUNT = 100
    fake_ds = generate_fake_dataset(ROW_COUNT, columns=dataset.get_columns(metadata))

    fake_ds.loc[0, "nullable_ownership"] = ""

    results = validate.validate_df(fake_ds, dataset, metadata)
    assert (
        len(results) == 0
    ), "No errors should have been found for invalid standardized values"


def test_non_nullable_bbls():
    dataset = metadata.package.get_dataset("primary_csv")
    ROW_COUNT = 100
    fake_ds = generate_fake_dataset(ROW_COUNT, columns=dataset.get_columns(metadata))

    fake_ds.loc[0, "bbl"] = ""

    results = validate.validate_df(fake_ds, dataset, metadata)
    assert len(results) == 1, "One error should have been found"

    error_type, error_msg = results[0]
    print(results[0])
    assert (
        error_type == validate.Errors.NULLS_FOUND
    ), "The error type should be NULLS_FOUND"


def test_invalid_wkbs():
    dataset = metadata.package.get_dataset("primary_csv")
    ROW_COUNT = 100
    fake_ds = generate_fake_dataset(ROW_COUNT, columns=dataset.get_columns(metadata))

    BAD_GEOM_VAL = "123"
    fake_ds.loc[0, "wkb_geometry"] = BAD_GEOM_VAL

    results = validate.validate_df(fake_ds, dataset, metadata)
    assert len(results) == 1, "One error should have been found"

    error_type, error_msg = results[0]
    assert (
        error_type == validate.Errors.INVALID_DATA
    ), "The correct error type should be returned"

    assert BAD_GEOM_VAL in error_msg


def test_additional_cols_in_source():
    dataset = metadata.package.get_dataset("primary_csv")
    ROW_COUNT = 100
    fake_ds = generate_fake_dataset(ROW_COUNT, columns=dataset.get_columns(metadata))

    FAKE_COL_NAME = "my_fake_col"
    fake_ds[FAKE_COL_NAME] = "4"

    results = validate.validate_df(fake_ds, dataset, metadata)
    assert len(results) == 1, "One error should have been found"

    error_type, error_msg = results[0]
    assert (
        error_type == validate.Errors.COLUMM_MISMATCH
    ), "The correct error type should be returned"

    assert (
        FAKE_COL_NAME in error_msg
    ), "The fake column name should be mentioned in the error message"
