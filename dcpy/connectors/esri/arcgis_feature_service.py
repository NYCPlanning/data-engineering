from datetime import datetime
import requests
from typing import cast
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)

from dcpy.models.connectors.esri import FeatureServer


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
