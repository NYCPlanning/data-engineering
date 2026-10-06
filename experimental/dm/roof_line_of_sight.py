# /// script
# dependencies = [
#     "altair",
#     "geopandas",
#     "leafmap",
#     "marimo",
#     "numpy",
#     "pandas",
#     "pyproj",
#     "requests",
#     "shapely",
# ]
# ///
import marimo

__generated_with = "0.23.16"
app = marimo.App(width="medium")

with app.setup:
    import altair as alt
    import geopandas as gpd
    import leafmap.maplibregl as leafmapgl
    import marimo as mo
    import numpy as np
    import pandas as pd
    import requests
    import shapely

    # NYC Open Data building footprints (5zhs-2jue), queried per run so only the corridor is downloaded
    FOOTPRINTS_URL = "https://data.cityofnewyork.us/resource/5zhs-2jue.geojson"
    # NY State Plane Long Island, in feet, so distances share units with the height columns
    CRS_FEET = "EPSG:2263"


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Roof-to-Roof Line of Sight

    Can someone on building A's roof see building B's roof past the buildings in between?

    Each building is a flat-topped block rising to `ground_elevation + height_roof` (feet above sea level).
    A sight line is blocked where it passes through a block below that block's roof.
    Eye height is added on A only.

    Both of these are tested:

    - **Centers**: one line from near the middle of A's roof to near the middle of B's. A and B themselves don't block it.
    - **Edges**: lines between points spaced around both roof edges. A and B can block these, so a line from the far side of a roof is blocked by that roof. B is visible if any line is clear.

    Not modeled: setbacks (each building rises straight up from its whole footprint), rooftop structures like bulkheads and water tanks, and trees.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.callout(
        mo.md(
            "Building footprints, heights, and ground elevations come from the "
            "[BUILDING](https://data.cityofnewyork.us/City-Government/BUILDING/5zhs-2jue/about_data) "
            "dataset on NYC Open Data, published by the Office of Technology and Innovation. "
            "Each run queries it live for just the two buildings and the corridor between them."
        ),
        kind="info",
    )
    return


@app.cell
def _():
    bin_a = mo.ui.text(value="4005003", label="Building A BIN")
    bin_b = mo.ui.text(value="4617821", label="Building B BIN")
    eye_height = mo.ui.number(
        value=5.5, start=0, step=0.5, label="Eye height above roof (ft)"
    )
    spacing = mo.ui.number(value=10, start=1, step=1, label="Edge sample spacing (ft)")
    mo.hstack([bin_a, bin_b, eye_height, spacing], justify="start")
    return bin_a, bin_b, eye_height, spacing


@app.function
def load_buildings(where: str) -> gpd.GeoDataFrame:
    """Read footprints matching a SoQL WHERE clause, projected to feet."""
    limit = 50_000
    response = requests.get(
        FOOTPRINTS_URL,
        params={
            "$select": "bin, name, height_roof, ground_elevation, the_geom",
            "$where": where,
            "$limit": limit,
        },
        timeout=60,
    )
    response.raise_for_status()
    features = response.json()["features"]
    assert len(features) < limit, "query hit the row limit; results are truncated"
    gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326").to_crs(CRS_FEET)
    # Socrata returns every property as a string
    gdf["bin"] = gdf.bin.astype(int)
    for col in ["height_roof", "ground_elevation"]:
        gdf[col] = pd.to_numeric(gdf[col])
    gdf["roof_z"] = gdf.ground_elevation + gdf.height_roof
    return gdf


@app.cell
def _(bin_a, bin_b):
    endpoints = load_buildings(f"bin IN ({int(bin_a.value)}, {int(bin_b.value)})")
    building_a = endpoints[endpoints.bin == int(bin_a.value)].iloc[0]
    building_b = endpoints[endpoints.bin == int(bin_b.value)].iloc[0]

    def _row(label, color, b):
        _name = b["name"] if isinstance(b["name"], str) else ""
        return (
            f"| <span style='color:{color}'>&#9632;</span> **{label}** | {b.bin} | {_name} "
            f"| {b.height_roof:,.0f} ft | {b.ground_elevation:,.0f} ft | {b.roof_z:,.0f} ft |"
        )

    mo.md(
        "\n".join(
            [
                "| | BIN | Name | Height | Ground elevation | Roof elevation |",
                "|---|---|---|--:|--:|--:|",
                _row("A", "#3b7dd8", building_a),
                _row("B", "#e8a33d", building_b),
            ]
        )
    )
    return building_a, building_b


@app.cell
def _(bin_a, bin_b, building_a, building_b):
    corridor = shapely.convex_hull(
        shapely.union(building_a.geometry, building_b.geometry)
    )
    # buffered so footprints touching the hull edge aren't lost to the round trip through 4326
    corridor_4326 = (
        gpd.GeoSeries([corridor.buffer(5)], crs=CRS_FEET).to_crs("EPSG:4326").iloc[0]
    )
    blockers = load_buildings(f"intersects(the_geom, '{corridor_4326.wkt}')")
    blockers = blockers[blockers.intersects(corridor)].reset_index(drop=True)
    blockers["is_endpoint"] = blockers.bin.isin([int(bin_a.value), int(bin_b.value)])

    _missing = blockers.roof_z.isna().sum()
    mo.output.append(
        mo.md(
            f"**{len(blockers):,}** buildings are in the corridor between A and B "
            "(the convex hull of the two footprints), the only area where a building can block a sight line."
        )
    )
    if _missing:
        mo.output.append(
            mo.callout(
                mo.md(
                    f"{_missing} of them have no height or ground elevation and can't block anything."
                ),
                kind="warn",
            )
        )
    return blockers, corridor


@app.function
def roof_edge_points(geom, spacing: float) -> np.ndarray:
    """XY points every `spacing` along the exterior ring of each polygon part."""
    points = []
    for part in getattr(geom, "geoms", [geom]):
        ring = part.exterior
        n = max(int(ring.length // spacing), 4)
        distances = np.linspace(0, ring.length, n, endpoint=False)
        points.append(
            shapely.get_coordinates(shapely.line_interpolate_point(ring, distances))
        )
    return np.vstack(points)


@app.function
def sight_line_visibility(
    a_xy: np.ndarray,
    a_z: float,
    b_xy: np.ndarray,
    b_z: float,
    footprints: np.ndarray,
    roof_z: np.ndarray,
    bins: np.ndarray,
) -> gpd.GeoDataFrame:
    """Test a line from every point in `a_xy` to every point in `b_xy` against flat-roofed prisms.

    `clearance_ft` is the tightest gap between the line and any roof it passes over
    (negative = blocked, NaN = passes over nothing). `blocked_by` is the BIN of the
    first blocking building walking from A.
    """
    ia, ib = (
        i.ravel()
        for i in np.meshgrid(np.arange(len(a_xy)), np.arange(len(b_xy)), indexing="ij")
    )
    lines = shapely.linestrings(np.stack([a_xy[ia], b_xy[ib]], axis=1))

    # a line's height is linear along it and roofs are flat, so checking where it
    # enters and leaves each footprint is exact
    line_idx, fp_idx = shapely.STRtree(footprints).query(lines, predicate="intersects")
    crossings = shapely.intersection(lines[line_idx], footprints[fp_idx])
    xy, k = shapely.get_coordinates(crossings, return_index=True)
    line_idx, fp_idx = line_idx[k], fp_idx[k]
    t = shapely.line_locate_point(lines[line_idx], shapely.points(xy), normalized=True)

    hits = pd.DataFrame(
        {
            "line": line_idx,
            "t": t,
            "clearance": a_z + t * (b_z - a_z) - roof_z[fp_idx],
            "bin": bins[fp_idx],
        }
    )
    # a line touches its own two roofs at t = 0 and t = 1, which isn't an obstruction
    hits = hits[(hits.t > 1e-6) & (hits.t < 1 - 1e-6)]
    clearance = hits.groupby("line").clearance.min()
    blocked_by = hits[hits.clearance < 0].sort_values("t").groupby("line").bin.first()

    out = gpd.GeoDataFrame({"a": ia, "b": ib}, geometry=lines, crs=CRS_FEET)
    out["clearance_ft"] = clearance.reindex(out.index)
    out["blocked_by"] = blocked_by.reindex(out.index).astype("Int64")
    out["visible"] = ~(out.clearance_ft < 0)
    return out


@app.cell
def _(blockers, building_a, building_b, eye_height, spacing):
    _z_a = building_a.roof_z + eye_height.value
    _z_b = building_b.roof_z

    _others = blockers[~blockers.is_endpoint]
    center = sight_line_visibility(
        shapely.get_coordinates(building_a.geometry.representative_point()),
        _z_a,
        shapely.get_coordinates(building_b.geometry.representative_point()),
        _z_b,
        _others.geometry.to_numpy(),
        _others.roof_z.to_numpy(),
        _others.bin.to_numpy(),
    )
    edges = sight_line_visibility(
        roof_edge_points(building_a.geometry, spacing.value),
        _z_a,
        roof_edge_points(building_b.geometry, spacing.value),
        _z_b,
        blockers.geometry.to_numpy(),
        blockers.roof_z.to_numpy(),
        blockers.bin.to_numpy(),
    )
    return center, edges


@app.cell
def _(building_a, center, edges, spacing):
    _n_clear = int(edges.visible.sum())
    _center_visible = bool(center.visible.iloc[0])
    # edge points are evenly spaced, so the share of points with a view approximates the share of edge length
    _perimeter = sum(
        _part.exterior.length
        for _part in getattr(building_a.geometry, "geoms", [building_a.geometry])
    )
    _edge_with_view = edges.groupby("a").visible.any().mean() * _perimeter

    mo.hstack(
        [
            mo.stat(
                "Yes" if _n_clear else "No",
                label="Visible from an edge",
                caption=f"{_n_clear:,} of {len(edges):,} edge-to-edge lines are clear",
                bordered=True,
            ),
            mo.stat(
                "Yes" if _center_visible else "No",
                label="Visible from the centers",
                caption=(
                    "the center-to-center line is clear"
                    if _center_visible
                    else f"blocked by BIN {center.blocked_by.iloc[0]}"
                ),
                bordered=True,
            ),
            mo.stat(
                f"{_edge_with_view:,.0f} of {_perimeter:,.0f} ft",
                label="Roof edge on A with a view of B",
                caption=f"to within the {spacing.value:g} ft sample spacing",
                bordered=True,
            ),
        ],
        widths="equal",
        gap=1,
    )
    return


@app.cell
def _(blockers, building_a, building_b, center, corridor, edges, eye_height):
    _ft_to_m = 0.3048
    _datum = blockers.ground_elevation.min()
    _color_a, _color_b = "#3b7dd8", "#e8a33d"
    _buildings = blockers.to_crs("EPSG:4326").assign(
        extrude_m=(blockers.roof_z.fillna(_datum) - _datum) * _ft_to_m,
        color=np.select(
            [blockers.bin == building_a.bin, blockers.bin == building_b.bin],
            [_color_a, _color_b],
            "#8a8f98",
        ),
    )
    _z = (
        np.array([building_a.roof_z + eye_height.value, building_b.roof_z]) - _datum
    ) * _ft_to_m
    _red, _green = [214, 69, 69], [46, 158, 91]

    def _with_paths(lines):
        """Add a 3D `path` (lng, lat, height above datum in m) and a tooltip label."""
        _xy = shapely.get_coordinates(lines.to_crs("EPSG:4326").geometry).reshape(
            -1, 2, 2
        )
        return lines.assign(
            path=[[[*_a, _z[0]], [*_b, _z[1]]] for _a, _b in _xy.tolist()],
            clearance=[
                "passes over no other roofs" if pd.isna(c) else f"clearance {c:.1f} ft"
                for c in lines.clearance_ft
            ],
        )

    def _path_layer(layer_id, lines, color, width):
        return {
            "@@type": "PathLayer",
            "id": layer_id,
            "data": lines[["path", "clearance"]].to_dict("records"),
            "getPath": "@@=path",
            "getColor": color,
            # screen-facing lines of fixed pixel width, not flat ribbons in meters
            "billboard": True,
            "widthUnits": "pixels",
            "getWidth": width,
            "pickable": True,
        }

    def _roof_points(lines, end):
        """One dot per sampled roof point, green if any sight line from it is clear."""
        _points = lines.groupby(end).agg(
            path=("path", "first"), visible=("visible", "any")
        )
        _i = 0 if end == "a" else 1
        return {
            "@@type": "ScatterplotLayer",
            "id": f"roof points {end}",
            "data": [
                {"position": p[_i], "color": _green if v else _red}
                for p, v in zip(_points.path, _points.visible)
            ],
            "getPosition": "@@=position",
            "getFillColor": "@@=color",
            "radiusUnits": "pixels",
            "getRadius": 3,
            "billboard": True,
        }

    _edges = _with_paths(edges)
    _center = _with_paths(center)
    _labels = [
        {
            "text": _name,
            "position": [
                *gpd.GeoSeries([_b.geometry.representative_point()], crs=CRS_FEET)
                .to_crs("EPSG:4326")
                .get_coordinates()
                .iloc[0],
                (_b.roof_z - _datum) * _ft_to_m + 15,
            ],
        }
        for _name, _b in [("A", building_a), ("B", building_b)]
    ]

    # look across the A-B line so A sits on the left and B on the right
    _ax, _ay = building_a.geometry.centroid.coords[0]
    _bx, _by = building_b.geometry.centroid.coords[0]
    _azimuth = np.degrees(np.arctan2(_bx - _ax, _by - _ay))
    _center_lng, _center_lat = (
        gpd.GeoSeries([corridor.centroid], crs=CRS_FEET)
        .to_crs("EPSG:4326")
        .get_coordinates()
        .iloc[0]
    )
    # zoom at which the corridor spans about 450 px (Web Mercator: 156,543 m/px at zoom 0)
    _span_m = 2 * shapely.minimum_bounding_radius(corridor) * _ft_to_m
    _zoom = np.log2(156_543 * np.cos(np.radians(_center_lat)) * 450 / _span_m)
    m = leafmapgl.Map(
        center=(_center_lng, _center_lat),
        zoom=float(_zoom),
        pitch=55,
        bearing=float((_azimuth - 90) % 360),
        style="positron",
        height="650px",
    )
    m.add_gdf(
        gpd.GeoDataFrame(geometry=[corridor], crs=CRS_FEET).to_crs("EPSG:4326"),
        layer_type="line",
        paint={"line-color": "#555", "line-dasharray": [2, 2]},
        name="corridor",
        fit_bounds=False,
    )
    m.add_gdf(
        _buildings[["bin", "name", "height_roof", "extrude_m", "color", "geometry"]],
        layer_type="fill-extrusion",
        paint={
            "fill-extrusion-color": ["get", "color"],
            "fill-extrusion-height": ["get", "extrude_m"],
            "fill-extrusion-opacity": 0.6,
        },
        name="buildings",
        fit_bounds=False,
    )
    # deck.gl paths take a z per vertex; MapLibre line layers are always drawn on the ground
    m.add_deck_layers(
        [
            _path_layer("blocked lines", _edges[~_edges.visible], [*_red, 40], 1),
            _path_layer("clear lines", _edges[_edges.visible], [*_green, 255], 2),
            _path_layer(
                "center line",
                _center,
                _green if _center.visible.iloc[0] else _red,
                6,
            ),
            _roof_points(_edges, "a"),
            _roof_points(_edges, "b"),
            {
                "@@type": "TextLayer",
                "id": "labels",
                "data": _labels,
                "getText": "@@=text",
                "getPosition": "@@=position",
                "getSize": 28,
                "getColor": [30, 30, 30],
                "fontWeight": "bold",
            },
        ],
        tooltip={
            "clear lines": "{{ clearance }}",
            "center line": "center: {{ clearance }}",
        },
    )
    m
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    A is blue and B is orange. Dots are sampled roof-edge points, green if any clear line touches them. The thick line is the center line.

    Heights are measured from the lowest ground in the corridor, so buildings and lines stay lined up on sloped ground.
    """)
    return


@app.cell(hide_code=True)
def _(blockers, building_a, building_b, center, edges):
    _roles = np.select(
        [blockers.bin == building_a.bin, blockers.bin == building_b.bin],
        ["A", "B"],
        "other",
    )
    # projected coordinates (feet) drawn as-is; SVG y runs down, so flip it
    _projection = {"type": "identity", "reflectY": True}

    def _lines_chart(lines, color, opacity, width):
        return (
            alt.Chart(lines[["geometry"]])
            .mark_geoshape(
                filled=False,
                stroke=color,
                strokeOpacity=opacity,
                strokeWidth=width,
            )
            .project(**_projection)
        )

    _buildings = (
        alt.Chart(blockers.assign(role=_roles)[["bin", "role", "roof_z", "geometry"]])
        .mark_geoshape(stroke="white", strokeWidth=0.5)
        .encode(
            color=alt.Color(
                "role:N",
                scale=alt.Scale(
                    domain=["A", "B", "other"],
                    range=["#3b7dd8", "#e8a33d", "#c4c7cc"],
                ),
                title=None,
            ),
            tooltip=["bin:N", "role:N", alt.Tooltip("roof_z:Q", format=".1f")],
        )
        .project(**_projection)
    )
    alt.layer(
        _buildings,
        _lines_chart(edges[~edges.visible], "#d64545", 0.06, 1),
        _lines_chart(edges[edges.visible], "#2e9e5b", 0.5, 1),
        _lines_chart(center, "#2e9e5b" if center.visible.iloc[0] else "#d64545", 1, 4),
    ).properties(width="container", height=450, title="Top-down view")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    The best edge-to-edge line and the roofs under it:
    """)
    return


@app.cell
def _(blockers, building_a, building_b, edges, eye_height):
    _best = edges.loc[edges.clearance_ft.fillna(np.inf).idxmax()]
    _line = _best.geometry

    # where the line enters and leaves each footprint, as distance from A
    _rows = []
    for _bldg in blockers[blockers.intersects(_line)].itertuples():
        _hit = _line.intersection(_bldg.geometry)
        for _part in getattr(_hit, "geoms", [_hit]):
            _d = [_line.project(shapely.Point(_xy)) for _xy in _part.coords]
            _rows.append(
                {
                    "bin": _bldg.bin,
                    "start_ft": min(_d),
                    "end_ft": max(_d),
                    "base": 0,
                    "roof_z": _bldg.roof_z,
                    "is_endpoint": _bldg.is_endpoint,
                }
            )
    _sight = pd.DataFrame(
        {
            "distance_ft": [0, _line.length],
            "elevation_ft": [building_a.roof_z + eye_height.value, building_b.roof_z],
        }
    )

    _roofs = (
        alt.Chart(pd.DataFrame(_rows))
        .mark_rect(opacity=0.7)
        .encode(
            x=alt.X("start_ft:Q", title="distance from A (ft)"),
            x2="end_ft:Q",
            y=alt.Y("roof_z:Q", title="elevation above sea level (ft)"),
            y2="base:Q",
            color=alt.condition(
                "datum.is_endpoint", alt.value("#e8a33d"), alt.value("#8a8f98")
            ),
            tooltip=["bin:N", alt.Tooltip("roof_z:Q", format=".1f")],
        )
    )
    _line_chart = (
        alt.Chart(_sight)
        .mark_line(color="#2e9e5b" if _best.visible else "#d64545", strokeWidth=2)
        .encode(x="distance_ft:Q", y="elevation_ft:Q")
    )
    (_roofs + _line_chart).properties(
        width="container",
        height=300,
        title=(
            "Best edge-to-edge line: passes over no other roofs"
            if pd.isna(_best.clearance_ft)
            else f"Best edge-to-edge line: clearance {_best.clearance_ft:.1f} ft"
        ),
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    A clearance of a few feet is within the error of the height data, so treat it as a maybe.
    """)
    return


if __name__ == "__main__":
    app.run()
