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
        st_startpoint(geom) AS start_point,
        st_endpoint(geom) AS end_point,
        st_lineinterpolatepoint(geom, 0.5) AS mid_point,
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
    st_x(center_of_curvature) AS center_of_curvature_x,
    st_y(center_of_curvature) AS center_of_curvature_y
FROM center_calculated
