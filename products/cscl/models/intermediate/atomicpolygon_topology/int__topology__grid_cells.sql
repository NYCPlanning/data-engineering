{{ config(
    materialized='table',
    indexes=[
        {'columns': ['x_cell', 'y_cell']},
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

-- One row per occupied grid cell, keyed by integer cell coordinates, with grid size =
-- ap_topology_node_merge_tolerance_ft. Since the grid is exactly the tolerance wide, two
-- points within tolerance of each other differ by at most one cell in x and in y, so a
-- cell's 8 neighbours are guaranteed to contain every true match
-- (int__topology__point_to_node checks them). This replaces windowed clustering
-- (ST_ClusterDBSCAN), which must hold a whole partition in memory: it can't run
-- citywide, and any partition just relocates the merge problem to the partition
-- boundary.

SELECT
    round(st_x(geom) / {{ var('ap_topology_node_merge_tolerance_ft') }})::bigint AS x_cell,
    round(st_y(geom) / {{ var('ap_topology_node_merge_tolerance_ft') }})::bigint AS y_cell,
    (array_agg(geom ORDER BY point_id))[1] AS geom,
    array_agg(point_id) AS exact_point_ids
FROM {{ ref('int__topology__exact_points') }}
GROUP BY
    round(st_x(geom) / {{ var('ap_topology_node_merge_tolerance_ft') }})::bigint,
    round(st_y(geom) / {{ var('ap_topology_node_merge_tolerance_ft') }})::bigint
