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
fit_checked AS (
    SELECT
        *,
        curve_flag IN ('L', 'R') AND fitted_center IS NULL AS no_center_found,
        -- prod writes the rounded center into 7-character fields: a longer one (a center
        -- 10,000,000 ft or more east or north, 1,000,000 ft or more west or south of the
        -- origin, from a nearly straight arc) is invalid
        fitted_center IS NOT NULL
        AND (
            NOT isfinite(st_x(fitted_center))
            OR NOT isfinite(st_y(fitted_center))
            OR round_even(st_x(fitted_center), 0) NOT BETWEEN -999999 AND 9999999
            OR round_even(st_y(fitted_center), 0) NOT BETWEEN -999999 AND 9999999
        ) AS center_out_of_range
    FROM center_calculated
),
center_or_midpoint AS (
    -- With no circle (collinear start, middle and end points, CSCL-LOG-LION-08) or an
    -- invalid center (CSCL-LOG-LION-07), prod falls back to the segment midpoint.
    SELECT
        *,
        CASE
            WHEN curve_flag IN ('L', 'R')
                THEN CASE WHEN no_center_found OR center_out_of_range THEN midpoint ELSE fitted_center END
        END AS center_of_curvature
    FROM fit_checked
)
SELECT
    * EXCLUDE (fitted_center),
    -- the fitted circle, for CSCL-LOG-LION-07's message
    st_x(fitted_center) AS fitted_center_x,
    st_y(fitted_center) AS fitted_center_y,
    st_z(fitted_center) AS fitted_radius,
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
