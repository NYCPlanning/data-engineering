"""Check that a map config pulled with `carto maps get` opens in its default state.

Builder saves whatever its tab is showing (view, widget selections, open widgets) as
everyone's starting state, so run this on every pull before committing or publishing.
Which widgets start open, and their order, are checked against the committed copy.

Usage: python check_start_state.py lift_tracker_dev_2.json
"""

import json
import subprocess
import sys

# The committed whole-city views sit between zoom 9.5 and 9.9.
MAX_ZOOM = 10.5
NYC_BOUNDS = {"latitude": (40.49, 40.92), "longitude": (-74.26, -73.69)}


def default_selection(param: dict) -> list:
    if param.get("selectionMode") == "single":
        return [param["defaultValue"]]
    # "(none)" is a pick that matches nothing, for parameters that hide what's picked.
    if "(none)" in param["values"]:
        return ["(none)"]
    return param["values"]


def is_expanded(widget: dict) -> bool:
    return widget.get("wrapperProps", {}).get("expanded", True)


def problems(bundle: dict, committed: dict) -> list[str]:
    config = bundle["keplerMapConfig"]["config"]
    found = []

    view = config["mapState"]
    if view["zoom"] > MAX_ZOOM:
        found.append(f"zoomed in to {view['zoom']:.1f}, not the whole city")
    for axis, (low, high) in NYC_BOUNDS.items():
        if not low <= view[axis] <= high:
            found.append(f"view centered outside NYC ({axis} {view[axis]})")

    if config.get("spatialFilter"):
        found.append("an area selection is drawn")
    if config["visState"]["filters"]:
        found.append("layer filters are set")
    labels = {d["id"]: d["label"] for d in bundle["datasets"]}
    for dataset_id, selection in config["filters"].items():
        for column in selection:
            found.append(
                f"'{labels.get(dataset_id, dataset_id)}' is filtered on {column}"
            )

    for layer in config["visState"]["layers"]:
        label = layer["config"]["label"]
        if "District" in label and layer["config"]["isVisible"]:
            found.append(f"boundary layer '{label}' is on")

    committed_widgets = committed["keplerMapConfig"]["config"]["widgets"]
    if [w["id"] for w in config["widgets"]] != [w["id"] for w in committed_widgets]:
        found.append("widgets differ from the committed order")
    should_expand = {w["id"]: is_expanded(w) for w in committed_widgets}
    for widget in config["widgets"]:
        expected = should_expand.get(widget["id"])
        if expected is not None and is_expanded(widget) != expected:
            state = "expanded" if is_expanded(widget) else "collapsed"
            found.append(f"widget '{widget['title']}' is {state}")

    ui = config["uiState"]
    if not ui["controlsPaneOpen"] or ui["controlsPaneTab"] != "widgets":
        found.append("widget panel isn't open")
    committed_ui = committed["keplerMapConfig"]["config"]["uiState"]
    for key in sorted(ui.keys() | committed_ui.keys()):
        if ui.get(key) != committed_ui.get(key):
            found.append(
                f"uiState.{key} is {ui.get(key)}, committed {committed_ui.get(key)}"
            )

    for param in config["sqlParameters"]:
        expected = default_selection(param)
        if param["item"]["value"] != expected:
            found.append(f"parameter '{param['name']}' isn't at its default")
        sql_name = param["item"]["sqlName"]
        for dataset in bundle["datasets"]:
            bound = (dataset.get("queryParameters") or {}).get(sql_name)
            if bound is not None and bound != expected:
                found.append(
                    f"dataset '{dataset['label']}' binds '{sql_name}' to a non-default value"
                )

    return found


if __name__ == "__main__":
    path = sys.argv[1]
    with open(path) as f:
        pulled = json.load(f)
    committed = json.loads(
        subprocess.run(
            ["git", "show", f"HEAD:./{path}"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    )
    found = problems(pulled, committed)
    for problem in found:
        print(problem)
    sys.exit(1 if found else 0)
