from typing import Callable
from faker import Faker
from pandas import DataFrame as df
from pathlib import Path
import random
from shapely import wkb, wkt
import uuid

import dcpy.models.product.dataset.metadata_v2 as md
from dcpy.lifecycle.package import validate

METADATA_PATH = (
    Path(__file__).parent.resolve() / "resources" / "test_package" / "metadata.yml"
)
print(METADATA_PATH)
assert METADATA_PATH.exists()

rd = random.Random()
rd.seed(0)
faker = Faker()

metadata = md.Metadata.from_path(METADATA_PATH)


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


fakes: dict[str, Callable] = {
    "geometry": DCPFakes.wkt,
    "wkt": DCPFakes.wkt,
    "wkb": DCPFakes.wkb,
    "boro_code": DCPFakes.boro_code,
    "block": DCPFakes.block,
    "lot": DCPFakes.lot,
    "bbl": DCPFakes.bbl,
    "uid": lambda: str(uuid.UUID(int=rd.getrandbits(128), version=4)),
    "text": faker.pystr,
    "integer": lambda: str(faker.pyint()),
    "decimal": lambda: str(faker.pyfloat()),
    "latitude": lambda: str(faker.latitude()),
    "longitude": lambda: str(faker.longitude()),
}


def _fake_row(columns: list[md.DatasetColumn]):
    row = {}

    found_bbl_parts = {}
    bbl_parts = {"boro_code", "block", "lot"}
    found_bbl_name = ""
    for c in columns:
        if c.data_type == "bbl":
            found_bbl_name = c.name or ""
        elif c.values:
            row[c.name] = random.choice(c.values).value
        else:
            val = fakes[c.data_type or ""]()
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
        if c.checks and not c.checks.non_nullable and random.choice([True, False]):
            # adding some extra chaos
            if random.choice([True, False]):
                del row[c.name]
            else:
                row[c.name] = ""
    return row


def generate_fake_dataset(row_count: int, columns: list[md.DatasetColumn]):
    return df.from_records([_fake_row(columns) for i in range(row_count)])


# AR note: I mostly use this to conveniently get a fake dataset
# in a Jupyter Notebook.
def _generate_fake_dataset_from_test_md(row_count: int):
    return generate_fake_dataset(row_count, columns=metadata.columns)


def test_validating_valid_data():
    fake_ds = _generate_fake_dataset_from_test_md(100)
    results = validate.validate_df(fake_ds, metadata.columns)

    assert not results, "No Errors should have been found in source data"


def test_invalid_standardized_values():
    ROW_COUNT = 100
    fake_ds = generate_fake_dataset(ROW_COUNT, columns=metadata.columns)

    INVALID_OWNERSHIP_VALUES = ["X", "Y", "Z"]
    fake_ds.loc[0, "Owner type"] = INVALID_OWNERSHIP_VALUES[0]
    fake_ds.loc[1, "Owner type"] = INVALID_OWNERSHIP_VALUES[1]
    fake_ds.loc[2:, "Owner type"] = INVALID_OWNERSHIP_VALUES[2]

    errors = validate.validate_df(fake_ds, metadata.columns)
    assert (
        len(errors) == 1
    ), "One error should have been found for invalid standardized values"

    # Assert that the error message should mention the invalid values with their counts
    result_msg = errors[0].message
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
    ROW_COUNT = 100
    fake_ds = generate_fake_dataset(ROW_COUNT, columns=metadata.columns)

    fake_ds.loc[0, "Nullable Owner type"] = ""

    errors = validate.validate_df(fake_ds, metadata.columns)
    assert (
        not errors
    ), "No errors should have been found for invalid standardized values"


def test_non_nullable_bbls():
    ROW_COUNT = 100
    fake_ds = generate_fake_dataset(ROW_COUNT, columns=metadata.columns)

    fake_ds.loc[0, "BBL"] = ""

    errors = validate.validate_df(fake_ds, metadata.columns)
    assert len(errors) == 1, "One error should have been found"

    assert (
        errors[0].error_type == validate.ErrorType.NULLS_FOUND
    ), "The error type should be NULLS_FOUND"


# TODO: revisit after determining how to specify geometry types in md
# def test_invalid_wkbs():
#     ROW_COUNT = 100
#     fake_ds = generate_fake_dataset(ROW_COUNT, columns=metadata.columns)

#     BAD_GEOM_VAL = "123"
#     fake_ds.loc[0, "wkb_geometry"] = BAD_GEOM_VAL

#     results = validate.validate_df(fake_ds, dataset, metadata).errors
#     assert len(results) == 1, "One error should have been found"

#     assert (
#         results[0].error_type == validate.ErrorType.INVALID_DATA
#     ), "The correct error type should be returned"

#     assert BAD_GEOM_VAL in results[0].message


def test_additional_cols_in_source():
    ROW_COUNT = 100
    fake_ds = generate_fake_dataset(ROW_COUNT, columns=metadata.columns)

    FAKE_COL_NAME = "my_fake_col"
    fake_ds[FAKE_COL_NAME] = "4"

    errors = validate.validate_df(fake_ds, metadata.columns)
    assert len(errors) == 1, "One error should have been found"

    assert (
        errors[0].error_type == validate.ErrorType.COLUMM_MISMATCH
    ), "The correct error type should be returned"

    assert (
        FAKE_COL_NAME in errors[0].message
    ), "The fake column name should be mentioned in the error message"
