{{ config(
    materialized='table'
) }}

-- One row per topology arc edge: the control points its geometry was linearized from
-- (int__topology__edges_unnoded's canonical curve: start and end at the node coordinates,
-- the mid as chosen there). District layers built from the topology are re-curved from
-- these at export (recipe exports custom.arcs).

SELECT DISTINCT ON (node_lo, node_hi)
    node_lo,
    node_hi,
    st_x(st_startpoint(geom)) AS x0,
    st_y(st_startpoint(geom)) AS y0,
    st_x(arc_mid) AS x1,
    st_y(arc_mid) AS y1,
    st_x(st_endpoint(geom)) AS x2,
    st_y(st_endpoint(geom)) AS y2
FROM {{ ref('int__topology__edges') }}
WHERE is_arc
ORDER BY node_lo, node_hi
