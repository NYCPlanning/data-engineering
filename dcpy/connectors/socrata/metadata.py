from pathlib import Path
import typer
from typing import Any

from dcpy.connectors.socrata import publish as pub
from dcpy.connectors.socrata import metadata

import dcpy.models.product.dataset.metadata as md
from dcpy.utils.logging import logger

soc_types_to_dcp_types = {
    "checkbox": "boolean",
    "text": "text",
    "calendar_date": "datetime",
}
soc_geom_types = {"multipolygon", "point", "polygon"}

FILL_ME_IN_PLACEHOLDER = "<FILL ME IN>"


def make_dcp_col(c: pub.Socrata.Responses.Column) -> md.DatasetColumn:
    dcp_col: dict[str, Any] = {
        "id": c["fieldName"],
        "name": c["name"],
        "description": c.get("description", FILL_ME_IN_PLACEHOLDER),
        "data_type": FILL_ME_IN_PLACEHOLDER,
        "custom": {},
    }

    samples = c.get("cachedContents", {}).get("top", [])
    sample = samples[0].get("item") if samples else None

    if c["renderTypeName"] == "number":
        if isinstance(sample, float):
            dcp_col["data_type"] = "decimal"
        elif isinstance(sample, int):
            dcp_col["data_type"] = "integer"
        dcp_col["example"] = str(sample)
    elif c["renderTypeName"] in soc_geom_types:
        dcp_col["data_type"] = "geometry"
        dcp_col["custom"]["geometry_type"] = c["renderTypeName"]
        # Unsupported target for indexed assignment ("Collection[str]")  [index]
        # Note: don't set the example, as we don't really know what to do with geom types
    elif c["renderTypeName"] in soc_types_to_dcp_types:
        dcp_col["data_type"] = soc_types_to_dcp_types[c["renderTypeName"]]
        dcp_col["example"] = str(sample)

    ENTIRELY_ARBITRARY_MAX_STANDARDIZED_VALS = 10
    if (
        samples
        and len(samples) < ENTIRELY_ARBITRARY_MAX_STANDARDIZED_VALS
        and len(samples) == int(c["cachedContents"].get("cardinality", -1))
    ):
        dcp_col["values"] = [
            {"value": s["item"], "description": FILL_ME_IN_PLACEHOLDER} for s in samples
        ]
    dataset_column = md.DatasetColumn.model_construct(**dcp_col)

    # model_construct() method doesn't perform validation on keys, need this sanity check here
    # instance keys == column model keys below:
    assert dataset_column.__dict__.keys() == dataset_column.model_fields.keys(), (
        "DatasetColumn instance keys don't match the DatasetColumn class keys"
    )

    return dataset_column


def _slugify(s):
    """transform a messy string into a slug.
    e.g. 'MY* _weird. product- -name' -> 'my_weird_product_name'
    """
    return (
        "".join(ch for ch in s if ch.isalnum() or ch == " ").replace(" ", "_").lower()
    )


def make_dcp_metadata(socrata_md) -> md.Metadata:
    columns = [make_dcp_col(c) for c in socrata_md["columns"]]

    return md.Metadata(
        id=_slugify(
            socrata_md["resourceName"]
            if "resourceName" in socrata_md
            else socrata_md["name"]
        ),
        attributes=md.DatasetAttributes(
            display_name=socrata_md["name"],
            description=socrata_md["description"],
            tags=socrata_md.get("tags", []),
            each_row_is_a=socrata_md["metadata"].get("rowLabel")
            or FILL_ME_IN_PLACEHOLDER,
        ),
        columns=columns,
        destinations=[
            md.DestinationWithFiles(
                id="socrata_prod",
                type="socrata",
                files=[],
                custom={
                    "four_four": socrata_md["id"],
                },
            ),
        ],
        files=[
            md.FileAndOverrides(
                file=md.File(
                    id=FILL_ME_IN_PLACEHOLDER,
                    filename=FILL_ME_IN_PLACEHOLDER,
                    type="shapefile",
                )
            )
        ]
        + [
            md.FileAndOverrides(
                file=md.File(id=FILL_ME_IN_PLACEHOLDER, filename=a["filename"])
            )
            for a in socrata_md["metadata"].get("attachments", [])
        ],
    )


app = typer.Typer(add_completion=False)


@app.command("export")
def _export_socrata_metadata(
    socrata_domain: str,
    four_four: str,
    output_path: Path = typer.Option(
        None,
        "--output-path",
        "-o",
        help="Path to export the metadata to",
    ),
):
    logger.info(f"fetching metadata for {four_four}")

    output_path = output_path or Path(f"{four_four}.yml")

    logger.info(f"exporting {four_four} metadata to {output_path}")
    metadata.make_dcp_metadata(
        pub.Dataset(socrata_domain=socrata_domain, four_four=four_four).fetch_metadata()
    ).write_to_yaml(output_path)
