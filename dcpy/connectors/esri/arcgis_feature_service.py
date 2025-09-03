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
import json

from dcpy.models.connectors.esri import FeatureServer, FeatureServerLayer, Server
from dcpy.models.product.dataset import metadata
from dcpy.utils.logging import logger
from dcpy.connectors.registry import Connector


def get_feature_server_metadata(feature_server: FeatureServer) -> dict:
    """Given a FeatureServer, return its metadata as a dictionary"""
    resp = requests.get(f"{feature_server.url}", params={"f": "pjson"})
    resp.raise_for_status()
    error = resp.json().get("error")  # 200 responses might contain error details
    if error:
        raise Exception(f"Error fetching ESRI Server metadata: {error}")
    return resp.json()


def get_feature_server_layers(
    feature_server: FeatureServer,
) -> list[FeatureServerLayer]:
    """Given a FeatureServer, look up and return its available layers"""
    resp = get_feature_server_metadata(feature_server)
    return [
        FeatureServerLayer(
            server=feature_server.server,
            name=feature_server.name,
            layer_name=layer["name"],
            layer_id=layer["id"],
        )
        for layer in resp["layers"]
    ]


def resolve_layer(
    feature_server: FeatureServer,
    layer_name: str | None = None,
    layer_id: int | None = None,
) -> FeatureServerLayer:
    """
    Given a FeatureServer, and optional layer name or id, resolve layer information
    There are a few different modes depending on what is provided
    For all modes, layers for the feature_server are looked up. Then, if
    - layer_name and layer_id provided - validate layer exists, return it
    - layer_name or layer_id provided - lookup layer by provided key
    - neither provided - if feature_server has single layer, return it. Otherwise, error

    `assert` statements can hopefully be dropped - known bug in mypy to not correctly
    narrow types within tuples. See final comments in https://github.com/python/mypy/issues/12364
    """
    layers = get_feature_server_layers(feature_server)
    layer_labels = [layer.layer_label for layer in layers]

    match layer_id, layer_name:
        case None, None:
            if len(layers) > 1:
                raise ValueError(
                    f"Feature server {feature_server} has mulitple layers: {layer_labels}"
                )
            elif len(layers) == 0:
                raise LookupError(f"Feature server {feature_server} has no layers")
            else:
                return layers[0]
        case _, None:
            assert layer_id is not None
            layers_by_id = {layer.layer_id: layer for layer in layers}
            if layer_id in layers_by_id:
                return layers_by_id[layer_id]
            else:
                raise LookupError(
                    f"Layer with id {layer_id} not found in feature server {feature_server}. Found layers: {layer_labels}."
                )
        case None, _:
            assert layer_name is not None
            layers_by_name = {layer.layer_name: layer for layer in layers}
            if layer_name in layers_by_name:
                return layers_by_name[layer_name]
            else:
                raise LookupError(
                    f"Layer with name '{layer_name}' not found in feature server {feature_server}. Found layers: {layer_labels}."
                )
        case _:
            assert layer_name is not None
            assert layer_id is not None
            layer = FeatureServerLayer(
                server=feature_server.server,
                name=feature_server.name,
                layer_name=layer_name,
                layer_id=layer_id,
            )
            if layer not in layers:
                raise LookupError(
                    f"Layer '{layer}' not found in feature server {feature_server}"
                )
            return layer


def get_layer_metadata(layer: FeatureServerLayer) -> dict:
    """Given FeatureServerLayer, return its metadata as a dictionary"""
    resp = requests.get(layer.url, params={"f": "pjson"})
    resp.raise_for_status()
    error = resp.json().get("error")  # 200 responses might contain error details
    if error:
        raise Exception(f"Error fetching ESRI Server metadata: {error}")
    return resp.json()


def get_data_last_updated(layer: FeatureServerLayer) -> datetime:
    """Given FeatureServerLayer, lookup date of last data edit"""
    metadata = get_layer_metadata(layer)
    ## returned timestamp has milliseconds, fromtimestamp expects seconds
    return datetime.fromtimestamp(metadata["editingInfo"]["dataLastEditDate"] / 1e3)


def query_layer(layer: FeatureServerLayer, params: dict) -> dict:
    """
    Wrapper to query data for a FeatureServerLayer.
    Arguments are `layer`, a FeatureServerLayer, and `params`, which are kwargs for the api call

    For these params, we commonly use
    - where: essentially a sql where clause. Default of "1=1" should be provided
    - outFields: fields to select. Required when querying data. Default of "*" should be provided
    - outSr: spatial reference system to get data in
    - f: output format. Default of "geojson" should be provided
    - returnIdsOnly: boolean flag to only return ids. Doesn't have same query limits as data queries
    - objectIds: list of object ids (that can be queried separately) to return. Useful in

    Exhaustive list of possible params are here:
    https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer/#request-parameters
    """
    resp = requests.post(f"{layer.url}/query", data=params)
    resp.raise_for_status()
    return resp.json()


def get_layer(layer: FeatureServerLayer, crs: int, chunk_size=100) -> dict:
    """
    Given FeatureServerLayer and desired crs, fetches entire layer as geojson (dict)
    """
    params = {"where": "1=1", "outFields": "*", "outSr": crs, "f": "geojson"}

    # there is a limit of 2000 features on the server, unless we limit to objectIds only
    # so first, we get ids, then we chunk to get full layer
    obj_params = params.copy()
    obj_params["returnIdsOnly"] = True
    object_id_resp = query_layer(layer, obj_params)
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
            f"[green]Downloading [bold]{layer.name}[/bold]", total=len(object_ids)
        )

        def _downcase_properties_keys(feat):
            feat["properties"] = {k.lower(): v for k, v in feat["properties"].items()}
            return feat

        for i in range(0, len(object_ids), chunk_size):
            params["objectIds"] = object_ids[i : i + chunk_size]
            chunk = query_layer(layer, params)
            progress.update(task, completed=i + chunk_size)
            features += [_downcase_properties_keys(feat) for feat in chunk["features"]]

    return {"type": "FeatureCollection", "crs": crs, "features": features}


def download_layer(layer: FeatureServerLayer, crs: str, path: Path) -> None:
    geojson = get_layer(layer, crs=int(crs.strip("EPSG:")))
    with open(path, "w") as f:
        json.dump(geojson, f)


def make_dcp_metadata(layer_url: str) -> metadata.Metadata:
    if layer_url.endswith("FeatureServer/0"):
        layer_url = layer_url + "?f=pjson"

    resp = requests.get(layer_url).json()
    esri_to_dcp = {
        "esriFieldTypeString": "text",
        "esriFieldTypeDouble": "decimal",
        "esriFieldTypeSmallInteger": "integer",
    }

    raw_cols = resp.get("fields")
    our_cols = [
        metadata.DatasetColumn(
            id=c.get("name"),
            name=c.get("alias"),
            description="",
            data_type=esri_to_dcp[c.get("type")],  # type: ignore
        )
        for c in raw_cols
        if c["name"] != "OBJECTID"
    ]

    return metadata.Metadata(
        id=resp.get("name"),
        attributes=metadata.DatasetAttributes(
            display_name="FILL ME IN",
            description=resp.get("description"),
            tags=[],
            each_row_is_a="",
        ),
        files=[
            metadata.FileAndOverrides(
                file=metadata.File(
                    id="primary_shapefile",
                    filename="shapefile.zip",
                    type="shapefile",
                )
            )
        ],
        destinations=[
            metadata.DestinationWithFiles(
                id="socrata_prod",
                type="socrata",
            )
        ],
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

    output_path = output_path or Path(f"{md.id}.yml")

    logger.info(f"exporting metadata to {output_path}")
    with open(output_path, "w") as outfile:
        yaml.dump(md_json, outfile, sort_keys=False)


class ArcGISFeatureServiceConnector(Connector):
    conn_type: str = "arcgis_feature_server"

    def push(self, key: str, **kwargs) -> dict:
        raise NotImplementedError()

    def _pull(
        self,
        key: str,
        destination_path: Path,
        *,
        server: Server,
        layer_name: str | None = None,
        layer_id: int | None = None,
        **kwargs,
    ) -> dict:
        layer = resolve_layer(
            FeatureServer(server=server, name=key), layer_name, layer_id
        )
        output = destination_path / f"{layer.layer_name}.geojson"
        download_layer(layer, kwargs.get("crs", "EPSG:4326"), path=output)
        return {"path": output}

    def pull(self, key: str, destination_path: Path, **kwargs) -> dict:
        return self._pull(key, destination_path, **kwargs)

    def get_date_last_updated(
        self,
        key: str,
        *,
        server: Server,
        layer_name: str | None = None,
        layer_id: int | None = None,
        **kwargs,
    ) -> str:
        layer = resolve_layer(
            FeatureServer(server=server, name=key), layer_name, layer_id
        )
        return get_data_last_updated(layer).strftime("%Y%m%d")

    def get_latest_version(self, key, **kwargs) -> str:
        return self.get_date_last_updated(key, **kwargs)
