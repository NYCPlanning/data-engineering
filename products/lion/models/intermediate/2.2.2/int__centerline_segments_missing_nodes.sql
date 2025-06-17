{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}

WITH missing AS (
    SELECT * FROM {{ ref('int__centerline_segments_with_nodes') }}
    WHERE from_nodeid IS NULL or to_nodeid IS NULL
)

SELECT
    missing.segmentid,
    missing.from_nodeid AS identical_match_from_nodeid,
    missing.to_nodeid AS identical_match_to_nodeid,
    st_x(missing.from_geom) AS from_x,
    st_y(missing.from_geom) AS from_y,
    st_x(missing.to_geom) AS to_x,
    st_y(missing.to_geom) AS to_y,
    n_from.nodeid AS from_nodeid,
    n_to.nodeid AS to_nodeid,
    missing.from_sectionalmap,
    missing.to_sectionalmap
FROM missing
LEFT JOIN {{ source("recipe_sources", "dcp_cscl_nodes") }} AS n_from 
    ON ST_DWITHIN(missing.from_geom, n_from.geom, 0.1)
LEFT JOIN {{ source("recipe_sources", "dcp_cscl_nodes") }} AS n_to 
    ON ST_DWITHIN(missing.to_geom, n_to.geom, 0.1)
