{{ config(
    materialized = 'table'
) }}

WITH segments AS (
    SELECT
        globalid,
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
    segments.globalid,
    segments.lionkey,
    segments.segmentid,
    st_x(segments.from_geom) AS from_x,
    st_y(segments.from_geom) AS from_y,
    st_x(segments.to_geom) AS to_x,
    st_y(segments.to_geom) AS to_y,
    n_from.nodeid AS from_nodeid,
    n_to.nodeid AS to_nodeid,
    from_sm.sectional_map AS from_sectionalmap,
    to_sm.sectional_map AS to_sectionalmap,
    -- Bytes LION MapFrom/MapTo (ETL spec §2.7.3): the QuarterSectionalMap polygon's map
    -- number plus its quarter-sectional suffix, if any. lateral/limit 1 so overlapping
    -- polygons can't fan out rows.
    from_qsm.quarter_sectionalmap AS from_quarter_sectionalmap,
    to_qsm.quarter_sectionalmap AS to_quarter_sectionalmap
FROM segments
LEFT JOIN segments_to_nodes AS n_from
    ON segments.globalid = n_from.globalid AND n_from.direction = 'from'
LEFT JOIN segments_to_nodes AS n_to
    ON segments.globalid = n_to.globalid AND n_to.direction = 'to'
LEFT JOIN
    {{ source("recipe_sources", "dcp_cscl_sectionalmap") }} AS from_sm
    ON st_contains(from_sm.geom, segments.from_geom)
LEFT JOIN
    {{ source("recipe_sources", "dcp_cscl_sectionalmap") }} AS to_sm
    ON st_contains(to_sm.geom, segments.to_geom)
LEFT JOIN LATERAL (
    SELECT qsm.sectional_map || coalesce(qsm.sectional_qtr_suffix, '') AS quarter_sectionalmap
    FROM {{ source("recipe_sources", "dcp_cscl_quartersectionalmap") }} AS qsm
    WHERE st_contains(qsm.geom, segments.from_geom)
    ORDER BY qsm.sectional_map, qsm.sectional_qtr_suffix
    LIMIT 1
) AS from_qsm ON TRUE
LEFT JOIN LATERAL (
    SELECT qsm.sectional_map || coalesce(qsm.sectional_qtr_suffix, '') AS quarter_sectionalmap
    FROM {{ source("recipe_sources", "dcp_cscl_quartersectionalmap") }} AS qsm
    WHERE st_contains(qsm.geom, segments.to_geom)
    ORDER BY qsm.sectional_map, qsm.sectional_qtr_suffix
    LIMIT 1
) AS to_qsm ON TRUE
