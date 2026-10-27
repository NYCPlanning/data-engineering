{{ config(
    materialized = 'table'
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
        END AS fitted_center
    FROM points
),
center_or_midpoint AS (
    -- Collinear start, middle and end points have no circle: prod falls back to the
    -- segment midpoint and logs CSCL-LOG-LION-08.
    SELECT
        *,
        curve_flag IN ('L', 'R') AND fitted_center IS NULL AS center_is_midpoint,
        CASE
            WHEN curve_flag IN ('L', 'R') THEN coalesce(fitted_center, midpoint)
        END AS center_of_curvature
    FROM center_calculated
)
SELECT
    * EXCLUDE (fitted_center),
    st_x(center_of_curvature) AS center_of_curvature_x,
    st_y(center_of_curvature) AS center_of_curvature_y,
    -- Prod's radius is the distance from the rounded (integer-foot) center to the
    -- segment's start point, not the fitted circle's own radius (the Z coordinate
    -- find_circle() returns). The two round to different integers whenever the fitted
    -- radius is near .5 - about 25% of curved segments. This reproduces prod on every
    -- curved segment.
    st_distance(
        st_point(round_even(st_x(center_of_curvature), 0), round_even(st_y(center_of_curvature), 0)),
        start_point
    ) AS center_of_curvature_radius
FROM center_or_midpoint
