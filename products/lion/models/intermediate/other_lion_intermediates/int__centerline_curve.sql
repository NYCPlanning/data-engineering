{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}

WITH center_calculated AS (
    SELECT 
        segmentid,
        curve AS curve_flag,
        start_point,
        end_point,
        midpoint,
        geom,
        CASE
            WHEN curve_flag IN ('L', 'R') THEN
                find_circle(start_point, midpoint, end_point)
        END AS center_of_curvature
    FROM {{ ref("int__centerline") }}
    where curve <> 'S'
)
SELECT
    *,
    st_x(center_of_curvature) AS center_of_curvature_x,
    st_y(center_of_curvature) AS center_of_curvature_y
FROM center_calculated
