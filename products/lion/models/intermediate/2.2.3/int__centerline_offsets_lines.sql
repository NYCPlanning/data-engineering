-- int__centerline_offsets
-- takes centerline segment, finds adjoining atomic polygons
-- this is an approximation of the ETL docs method. See comments in #1568

{{ config(
    materialized = 'table'
) }}

WITH centerline AS (
    SELECT * FROM {{ ref("stg__centerline") }}
    -- TODO remove this -> likely from odd source data.
    WHERE segmentid NOT IN (
        354785, -- Large circular segment extending into other states
        9008702 -- Strange small simple line that is discontinuous
    )
),

parallel_lines AS (
    SELECT
        segmentid,
        boroughcode,
        st_offsetcurve(geom, 2, 'quad_segs=4') AS left_line,
        st_offsetcurve(geom, -2, 'quad_segs=4') AS right_line
    FROM centerline
),

offset_points AS (
    SELECT
        segmentid,
        boroughcode,
        left_line,
        right_line,
        st_lineinterpolatepoint(left_line, 0.5) AS left_offset_point,
        st_lineinterpolatepoint(right_line, 0.5) AS right_offset_point
    FROM parallel_lines
)

SELECT * FROM offset_points
