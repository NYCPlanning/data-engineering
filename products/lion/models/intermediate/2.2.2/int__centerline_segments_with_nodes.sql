{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}

WITH centerline AS (
    SELECT
        segmentid,
        st_startpoint(geom) AS from_geom,
        st_endpoint(geom) AS to_geom
    FROM {{ ref("stg__centerline") }}
)

SELECT
    centerline.segmentid,
    st_x(centerline.from_geom) AS from_x,
    st_y(centerline.from_geom) AS from_y,
    st_x(centerline.to_geom) AS to_x,
    st_y(centerline.to_geom) AS to_y,
    n_from.nodeid AS from_nodeid,
    n_to.nodeid AS to_nodeid,
    from_sm.sectional_map AS from_sectionalmap,
    to_sm.sectional_map AS to_sectionalmap
FROM centerline
-- ~75 segments are missing nodes
-- TODO research whether this is consistent with current method and if we might need to join spatially with some minimal buffer
LEFT JOIN {{ source("recipe_sources", "dcp_cscl_nodes") }} AS n_from ON centerline.from_geom = n_from.geom
LEFT JOIN {{ source("recipe_sources", "dcp_cscl_nodes") }} AS n_to ON centerline.to_geom = n_to.geom
-- 6 segments with endpoint on border. This currently results in duplicate rows
LEFT JOIN
    {{ source("recipe_sources", "dcp_cscl_sectionalmap") }} AS from_sm
    ON st_contains(from_sm.geom, centerline.from_geom)
LEFT JOIN
    {{ source("recipe_sources", "dcp_cscl_sectionalmap") }} AS to_sm
    ON st_contains(to_sm.geom, centerline.to_geom)
