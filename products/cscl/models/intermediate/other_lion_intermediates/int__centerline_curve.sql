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
    st_y(center_of_curvature) AS center_of_curvature_y,
    -- Prod's radius is the distance from the rounded (integer-foot) center to the
    -- segment's start point, not the fitted circle's own radius (the Z coordinate
    -- find_circle() returns). The two round to different integers whenever the fitted
    -- radius is near .5 - about 25% of curved segments. This reproduces prod on every
    -- curved segment (CSCL-LION-07, data_issues.md).
    st_distance(
        st_point(round_even(st_x(center_of_curvature), 0), round_even(st_y(center_of_curvature), 0)),
        start_point
    ) AS center_of_curvature_radius
FROM center_calculated
