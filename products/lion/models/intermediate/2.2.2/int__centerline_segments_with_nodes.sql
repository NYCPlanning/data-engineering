{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}

WITH centerline AS (
    SELECT
        segmentid,
        ST_StartPoint(geom) AS from_geom,
        ST_EndPoint(geom) AS to_geom
    FROM {{ ref("stg__centerline") }}
),

segments_to_nodes AS (
    SELECT * FROM {{ ref("int__segments_to_nodes") }}
)

SELECT
    centerline.segmentid,
    ST_X(centerline.from_geom) AS from_x,
    ST_Y(centerline.from_geom) AS from_y,
    ST_X(centerline.to_geom) AS to_x,
    ST_Y(centerline.to_geom) AS to_y,
    n_from.nodeid AS from_nodeid,
    n_to.nodeid AS to_nodeid,
    from_sm.sectional_map AS from_sectionalmap,
    to_sm.sectional_map AS to_sectionalmap
FROM centerline
LEFT JOIN segments_to_nodes AS n_from
    ON centerline.segmentid = n_from.segmentid AND n_from.direction = 'from'
LEFT JOIN segments_to_nodes AS n_to
    ON centerline.segmentid = n_to.segmentid AND n_to.direction = 'to'
LEFT JOIN
    {{ source("recipe_sources", "dcp_cscl_sectionalmap") }} AS from_sm
    ON ST_Contains(from_sm.geom, centerline.from_geom)
LEFT JOIN
    {{ source("recipe_sources", "dcp_cscl_sectionalmap") }} AS to_sm
    ON ST_Contains(to_sm.geom, centerline.to_geom)
