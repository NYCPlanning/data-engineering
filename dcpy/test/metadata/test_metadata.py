from faker import Faker
from pandas import DataFrame as df
from pathlib import Path
import random
from shapely import wkb, wkt
import uuid

from dcpy.metadata import models
from dcpy.metadata import validate
from dcpy.metadata.models import Metadata as md

METADATA_PATH = Path(__file__).parent.resolve() / "resources" / "template_md.yml"
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


def fake_row(columns: list[models.Column]):
    row = {}

    found_bbl_parts = {}
    bbl_parts = {"boro_code", "block", "lot"}
    for c in columns:
        if c.data_type == "bbl":
            pass
        else:
            val = fakes[c.data_type]()
            row[c.name] = val
            if c.data_type in {"boro_code", "block", "lot"}:
                found_bbl_parts[c.data_type] = val
    if set(found_bbl_parts.keys()) == bbl_parts:
        row["bbl"] = fakes["bbl"](
            found_bbl_parts["boro_code"],
            found_bbl_parts["block"],
            found_bbl_parts["lot"],
        )

    return row


def _generate_fake_dataset(row_count: int, columns: list[models.Column]):
    return df.from_records([fake_row(columns) for i in range(row_count)])


def test_socrata_column_overrides_wkb_geom():
    dataset = metadata.dataset_package.get_dataset("primary_shapefile")
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

    # CSV
    socrata_dest_cols_csv = metadata.get_destination(
        "socrata_csv"
    ).destination_column_metadata(metadata)
    socrata_dest_col_names_csv = {c.api_name for c in socrata_dest_cols_csv}

    assert "omit_me_from_shapefile" in socrata_dest_col_names_csv


def test_validating_valid_data():
    dataset = metadata.dataset_package.get_dataset("primary_shapefile")
    fake_ds = _generate_fake_dataset(100, columns=dataset.get_columns(metadata))
    results = validate.validate_df(fake_ds, dataset, metadata)

    assert results == [], "No Errors should have been found in source data"
