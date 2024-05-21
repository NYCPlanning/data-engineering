from pathlib import Path
import typer
import yaml

from dcpy.connectors.socrata import publish as pub
from dcpy.connectors.socrata import metadata

import dcpy.models.product.dataset.metadata as models
from dcpy.utils.logging import logger

soc_types_to_dcp_types = {
    "text": "text",
    "multipolygon": "wkb",
    "calendar_date": "datetime",
}


def make_dcp_col(c: pub.Socrata.Responses.Column):
    samples = c.get("cachedContents", {}).get("top", [])
    sample = "<FILL_ME_IN>"
    dcp_type = None

    if samples:
        sample = samples[0].get("item")

    if c["renderTypeName"] == "number":
        if type(sample) == float:
            dcp_type = "double"
        elif type(sample) == int:
            dcp_type = "integer"
        else:
            dcp_type = "<FILL_ME_IN>!"
    else:
        dcp_type = soc_types_to_dcp_types[c["renderTypeName"]]

    dcp_col = {
        "name": c["fieldName"],
        "display_name": c["name"],
        "description": c["description"],
        "data_type": dcp_type,
        "example": sample,
    }
    if c["renderTypeName"] == "multipolygon":
        dcp_col["readme_data_type"] = "geometry"

    ENTIRELY_ARBITRARY_MAX_STANDARDIZED_VALS = 10
    if (
        samples
        and len(samples) < ENTIRELY_ARBITRARY_MAX_STANDARDIZED_VALS
        and len(samples) == int(c["cachedContents"].get("cardinality"))  # type: ignore
    ):
        dcp_col["values"] = [
            [s["item"], "<FILL_IN_THE_VALUE_DESCRIPTION!>"] for s in samples
        ]  # type: ignore
    return models.Column(**dcp_col)  # type: ignore


def _slugify(s):
    """transform a messy string into a slug.
    e.g. 'MY* _weird. product- -name' -> 'my_weird_product_name'
    """
    return (
        "".join(ch for ch in s if ch.isalnum() or ch == " ").replace(" ", "_").lower()
    )


def make_dcp_metadata(socrata_md: pub.Socrata.Responses.Metadata) -> models.Metadata:
    columns = [make_dcp_col(c) for c in socrata_md["columns"]]

    return models.Metadata(
        name=_slugify(
            socrata_md["resourceName"]
            if "resourceName" in socrata_md
            else socrata_md["name"]
        ),
        display_name=socrata_md["name"],
        summary=socrata_md["description"],
        description=socrata_md["description"],
        tags=socrata_md["tags"],
        each_row_is_a=socrata_md["metadata"]["rowLabel"],
        columns=columns,
        destinations=[
            models.SocrataDestination(
                id="socrata_prod",
                four_four=socrata_md["id"],
                datasets=["primary_shapefile"],
                omit_columns=[],
            )
        ],
        package=models.Package(
            dataset_files=[
                models.DatasetFile(
                    name="primary_shapefile",
                    filename="shapefile.zip",
                    type="shapefile",
                    overrides=models.DatasetOverrides(),
                )
            ],
            attachments=[a["filename"] for a in socrata_md["metadata"]["attachments"]],
        ),
    )


app = typer.Typer(add_completion=False)


@app.command("export")
def _export_socrata_metadata(
    four_four: str,
    output_path: Path = typer.Option(
        None,
        "--output-path",
        "-o",
        help="Path to export the metadata to",
    ),
):
    logger.info(f"fetching metadata for {four_four}")
    md = metadata.make_dcp_metadata(
        pub.Dataset(four_four=four_four).fetch_metadata()
    ).model_dump(exclude_none=True)

    output_path = output_path or Path(f"{four_four}.yml")

    logger.info(f"exporting {four_four} metadata to {output_path}")
    with open(output_path, "w") as outfile:
        yaml.dump(md, outfile, sort_keys=False)


if __name__ == "__main__":
    app()
