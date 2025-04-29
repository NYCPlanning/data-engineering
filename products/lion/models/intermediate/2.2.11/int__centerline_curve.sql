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
center_calculated AS (
    SELECT
        segmentid,
        curve AS curve_flag,
        CASE
            WHEN curve IN ('L', 'R') THEN
                find_circle(
                    st_startpoint(geom),
                    st_endpoint(geom),
                    st_pointn(geom, st_numpoints(geom)/2)
                )
        END AS center_of_curvature
    FROM curves
)
SELECT 
    *,
    st_x(center_of_curvature) AS center_of_curvature_x,
    st_y(center_of_curvature) AS center_of_curvature_y
FROM center_calculated
