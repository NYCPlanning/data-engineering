{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}

WITH curves AS (
    SELECT * FROM {{ ref("stg__centerline") }}
    WHERE curve <> 'S'
),
segments AS (
    SELECT * FROM {{ ref("int__primary_segments") }}
),
points AS (
    SELECT
        curves.segmentid,
        curves.curve AS curve_flag,
        segments.start_point,
        segments.end_point,
        segments.midpoint,
        segments.geom
    FROM curves
    INNER JOIN segments ON curves.segmentid = segments.segmentid
),
center_calculated AS (
    SELECT
        segmentid,
        curve_flag,
        start_point,
        end_point,
        midpoint,
        geom,
        CASE
            WHEN curve_flag IN ('L', 'R') THEN
                find_circle(start_point, midpoint, end_point)
        END AS center_of_curvature
    FROM points
)
SELECT
    *,
    st_x(center_of_curvature) AS center_of_curvature_x,
    st_y(center_of_curvature) AS center_of_curvature_y
FROM center_calculated
