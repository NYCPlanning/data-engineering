{{ config(
    materialized='table',
    indexes=[
        {'columns': ['globalid']},
    ]
) }}

-- Every segment vertex (of the linearized geometry) that sits on an AP topology node,
-- with its index and distance along the segment. int__segment_topology_edge_matches
-- matches the stretch between consecutive ones to topology edges.
--
-- Snapping: source centerlines and APs share a 2^-14 ft grid, but ~19% of centerline
-- vertices sit 3-4 grid cells off the matching AP vertex, so a vertex takes the nearest
-- exact AP point within ap_topology_node_merge_tolerance_ft - the same tolerance that
-- merged those points into nodes. Endpoints that miss it are almost all > 2 ft from any
-- AP vertex (dangles, alleys, mid-edge T-junctions): a looser snap would buy little.
--
-- The lookup is one hash join on integer cell keys: cells are 4x the tolerance, and each
-- AP point is registered in every cell it lies within the tolerance of (1, 2 or 4
-- cells), so a vertex only needs to look in its own cell. Interior points of linearized
-- arcs aren't on the grid (on either side) and simply don't snap.

{% set tol = var('ap_topology_node_merge_tolerance_ft') %}
{% set cell = tol * 4 %}

WITH ap_points AS (
    SELECT
        ptn.node_id,
        st_x(p.geom) AS x,
        st_y(p.geom) AS y
    FROM {{ ref('int__topology__exact_points') }} AS p
    INNER JOIN {{ ref('int__topology__point_to_node') }} AS ptn ON p.point_id = ptn.exact_point_id
),

ap_point_cells AS (
    SELECT DISTINCT
        p.node_id,
        p.x,
        p.y,
        floor((p.x + ox.o * {{ tol }}) / {{ cell }})::bigint AS x_cell,
        floor((p.y + oy.o * {{ tol }}) / {{ cell }})::bigint AS y_cell
    FROM ap_points AS p
    CROSS JOIN (VALUES (-1), (1)) AS ox (o)
    CROSS JOIN (VALUES (-1), (1)) AS oy (o)
),

-- dumped and measured one segment at a time, so the window sorts ~14 rows by an integer
-- instead of 3M rows by globalid
vertices AS (
    SELECT
        s.globalid,
        d.idx,
        d.x,
        d.y,
        d.along_ft
    FROM {{ ref('int__segments') }} AS s
    CROSS JOIN LATERAL (
        SELECT
            idx,
            x,
            y,
            coalesce(sum(step_len) OVER (ORDER BY idx), 0) AS along_ft
        FROM (
            SELECT
                dp.path[1] AS idx,
                st_x(dp.geom) AS x,
                st_y(dp.geom) AS y,
                st_distance(dp.geom, lag(dp.geom) OVER (ORDER BY dp.path[1])) AS step_len
            FROM st_dumppoints(s.geom) AS dp
        ) AS steps
    ) AS d
),

candidates AS (
    SELECT
        v.globalid,
        v.idx,
        v.along_ft,
        p.node_id,
        (p.x - v.x) ^ 2 + (p.y - v.y) ^ 2 AS dist_sq
    FROM vertices AS v
    INNER JOIN ap_point_cells AS p
        ON
            floor(v.x / {{ cell }})::bigint = p.x_cell
            AND floor(v.y / {{ cell }})::bigint = p.y_cell
            AND (p.x - v.x) ^ 2 + (p.y - v.y) ^ 2 <= {{ tol }} ^ 2
),

-- nearest AP point per vertex, by grouping rather than DISTINCT ON: hash aggregates
-- stay cheap here, while sorting ~600k rows by globalid spills to disk
nearest AS (
    SELECT
        globalid,
        idx,
        min(dist_sq) AS dist_sq
    FROM candidates
    GROUP BY globalid, idx
)

SELECT
    c.globalid,
    c.idx,
    c.along_ft,
    min(c.node_id) AS node_id
FROM candidates AS c
INNER JOIN nearest AS n ON c.globalid = n.globalid AND c.idx = n.idx AND c.dist_sq = n.dist_sq
GROUP BY c.globalid, c.idx, c.along_ft
