from datetime import datetime
from pathlib import Path
import requests
import typer
from typing import cast
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)
import yaml

from dcpy.models.connectors.esri import FeatureServer
import dcpy.models.product.dataset.metadata as models
from dcpy.utils.logging import logger


def get_metadata(dataset: FeatureServer) -> dict:
    resp = requests.get(f"{dataset.url}", params={"f": "pjson"})
    resp.raise_for_status()
    error = resp.json().get("error")  # 200 responses might contain error details
    if error:
        raise Exception(f"Error fetching ESRI Server metadata: {error}")
    return resp.json()


def get_data_last_updated(dataset: FeatureServer) -> datetime:
    metadata = get_metadata(dataset)
    ## returned timestamp has milliseconds, fromtimestamp expects seconds
    return datetime.fromtimestamp(metadata["editingInfo"]["dataLastEditDate"] / 1e3)


def query_dataset(dataset: FeatureServer, params: dict) -> dict:
    resp = requests.post(f"{dataset.url}/query", data=params)
    resp.raise_for_status()
    return resp.json()


def get_dataset(dataset: FeatureServer, crs: int) -> dict:
    CHUNK_SIZE = 100
    params = {"where": "1=1", "outFields": "*", "outSr": crs, "f": "geojson"}

    # there is a limit of 2000 features on the server, unless we limit to objectIds only
    # so first, we get ids, then we chunk to get full dataset
    obj_params = params.copy()
    obj_params["returnIdsOnly"] = True
    object_id_resp = query_dataset(dataset, obj_params)
    object_ids = cast(list[int], object_id_resp["properties"]["objectIds"])

    features = []

    with Progress(
        SpinnerColumn(spinner_name="earth"),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=30),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
        transient=True,
    ) as progress:
        task = progress.add_task(
            f"[green]Downloading [bold]{dataset.name}[/bold]", total=len(object_ids)
        )

        def _downcase_properties_keys(feat):
            feat["properties"] = {k.lower(): v for k, v in feat["properties"].items()}
            return feat

        for i in range(0, len(object_ids), CHUNK_SIZE):
            params["objectIds"] = object_ids[i : i + CHUNK_SIZE]
            chunk = query_dataset(dataset, params)
            progress.update(task, completed=i + CHUNK_SIZE)
            features += [_downcase_properties_keys(feat) for feat in chunk["features"]]

    return {"type": "FeatureCollection", "crs": crs, "features": features}


def make_dcp_metadata(layer_url: str) -> models.Metadata:
    if layer_url.endswith("FeatureServer/0"):
        layer_url = layer_url + "?f=pjson"

    resp = requests.get(layer_url).json()
    esri_to_dcp = {
        "esriFieldTypeString": "text",
        "esriFieldTypeDouble": "double",
        "esriFieldTypeSmallInteger": "integer",
    }

    raw_cols = resp.get("fields")
    our_cols = [
        models.Column(
            name=c.get("name"),
            display_name=c.get("alias"),
            description="",
            data_type=esri_to_dcp[c.get("type")],
        )
        for c in raw_cols
        if c["name"] != "OBJECTID"
    ]

    return models.Metadata(
        name=resp.get("name"),
        display_name=models.FILL_ME_IN_PLACEHOLDER,
        package=models.Package(
            dataset_files=[
                models.DatasetFile(
                    name="primary_shapefile",
                    filename="shapefile.zip",
                    type="shapefile",
                    overrides=models.DatasetOverrides(),
                )
            ],
            attachments=[],
        ),
        destinations=[
            models.SocrataDestination(
                id="socrata_prod",
                four_four="",
                datasets=["primary_shapefile"],
                omit_columns=[],
            )
        ],
        summary="",
        description=resp.get("description"),
        tags=[],
        each_row_is_a="",
        columns=our_cols,
    )


metadata_app = typer.Typer(add_completion=False)

app = typer.Typer(add_completion=False)
app.add_typer(metadata_app, name="metadata")


@metadata_app.command("export")
def _export_metadata(
    layer_url: str,
    output_path: Path = typer.Option(
        None,
        "--output-path",
        "-o",
        help="Path to export the metadata to",
    ),
):
    logger.info(f"fetching metadata for {layer_url}")
    md = make_dcp_metadata(layer_url)
    md_json = md.model_dump(exclude_none=True)

    output_path = output_path or Path(f"{md.name}.yml")

    logger.info(f"exporting metadata to {output_path}")
    with open(output_path, "w") as outfile:
        yaml.dump(md_json, outfile, sort_keys=False)
