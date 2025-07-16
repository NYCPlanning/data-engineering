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
points AS (
    SELECT
        segmentid,
        curve AS curve_flag,
        ST_StartPoint(geom) AS start_point,
        ST_EndPoint(geom) AS end_point,
        ST_LineInterpolatePoint(geom, 0.5) AS mid_point,
        geom
    FROM curves
),
center_calculated AS (
    SELECT
        segmentid,
        curve_flag,
        start_point,
        end_point,
        mid_point,
        geom,
        CASE
            WHEN curve_flag IN ('L', 'R') THEN
                find_circle(start_point, mid_point, end_point)
        END AS center_of_curvature
    FROM points
)
SELECT
    *,
    ST_X(center_of_curvature) AS center_of_curvature_x,
    ST_Y(center_of_curvature) AS center_of_curvature_y
FROM center_calculated
