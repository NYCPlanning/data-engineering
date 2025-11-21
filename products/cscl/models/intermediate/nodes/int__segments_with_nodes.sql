{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}

WITH segments AS (
    SELECT
        lionkey,
        segmentid,
        st_startpoint(geom) AS from_geom,
        st_endpoint(geom) AS to_geom
    FROM {{ ref("int__segments") }}
),

segments_to_nodes AS (
    SELECT * FROM {{ ref("int__segments_to_nodes") }}
)

SELECT
    segments.lionkey,
    segments.segmentid,
    st_x(segments.from_geom) AS from_x,
    st_y(segments.from_geom) AS from_y,
    st_x(segments.to_geom) AS to_x,
    st_y(segments.to_geom) AS to_y,
    n_from.nodeid AS from_nodeid,
    n_to.nodeid AS to_nodeid,
    from_sm.sectional_map AS from_sectionalmap,
    to_sm.sectional_map AS to_sectionalmap
FROM segments
LEFT JOIN segments_to_nodes AS n_from
    ON segments.lionkey = n_from.lionkey AND n_from.direction = 'from'
LEFT JOIN segments_to_nodes AS n_to
    ON segments.lionkey = n_to.lionkey AND n_to.direction = 'to'
LEFT JOIN
    {{ source("recipe_sources", "dcp_cscl_sectionalmap") }} AS from_sm
    ON st_contains(from_sm.geom, segments.from_geom)
LEFT JOIN
    {{ source("recipe_sources", "dcp_cscl_sectionalmap") }} AS to_sm
    ON st_contains(to_sm.geom, segments.to_geom)
