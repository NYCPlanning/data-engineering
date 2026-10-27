-- CURVE is L or R but the center of curvature can't be used: the fitted center is too far
-- off for prod's 7-character fields (CSCL-LOG-LION-07), or there is no circle because the
-- start, middle and end points are collinear (CSCL-LOG-LION-08). Either way the center
-- falls back to the segment midpoint, as in prod. Once per centerline: proto segments copy
-- their centerline's curvature.
SELECT
    'warning' AS log_level,
    'center of curvature' AS error_category,
    'CSCL-LOG-LION-07' AS log_message_id,
    lion.globalid,
    lion.source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    lion.segmentid AS record_id,
    -- prod also prints the arc's central angle
    {{ log_message(
        "[INVALID ANGLE & RADIUS FOUND] An invalid angle & radius value was found while "
        "calculating the segment curvature for the {} feature with a GlobalID = {}.  "
        "Radius = {}, Calculated X = {}, Calculated Y = {}.  Using the segment midpoint for "
        "the curvature values.",
        ["lion.source_table", "lion.globalid", "curve.fitted_radius", "curve.fitted_center_x", "curve.fitted_center_y"]
    ) }} AS message
FROM {{ ref('int__lion') }} AS lion
INNER JOIN {{ ref('int__centerline_curve') }} AS curve ON lion.segmentid = curve.segmentid
WHERE
    lion.source_table = 'centerline'
    AND curve.center_out_of_range
UNION ALL
SELECT
    'warning' AS log_level,
    'center of curvature' AS error_category,
    'CSCL-LOG-LION-08' AS log_message_id,
    lion.globalid,
    lion.source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    lion.segmentid AS record_id,
    {{ log_message(
        "[NO CENTER POINT FOUND] Unable to locate a valid center point while calculating "
        "the segment curvature for the {} feature with a GlobalID = {}.  Using the segment "
        "midpoint for the curvature values.",
        ["lion.source_table", "lion.globalid"]
    ) }} AS message
FROM {{ ref('int__lion') }} AS lion
INNER JOIN {{ ref('int__centerline_curve') }} AS curve ON lion.segmentid = curve.segmentid
WHERE
    lion.source_table = 'centerline'
    AND curve.no_center_found
UNION ALL
-- CURVE is L or R but the center of curvature lies on the other side of the chord, or on
-- it (CenterlineExtractFile.cs:3307-3358): prod computes the side from the rounded start,
-- end and center coordinates it writes. Warned only. Features only: prod checks proto
-- segments elsewhere and doesn't make this check there.
SELECT
    'warning' AS log_level,
    'curve flag mismatch' AS error_category,
    'CSCL-LOG-LION-09' AS log_message_id,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    {{ log_message(
        "Curve ={};Curve_calcuated ={} for the {} feature with a GlobalID = {}.   SegmentID ={}",
        ["curve_flag", "calculated_curve", "source_table", "globalid", "segmentid"]
    ) }} AS message
FROM (
    SELECT
        *,
        CASE
            WHEN side > 0 THEN 'R'
            WHEN side < 0 THEN 'L'
            ELSE ' '
        END AS calculated_curve
    FROM (
        SELECT
            *,
            (from_x::BIGINT - center_of_curvature_x::BIGINT) * (to_y::BIGINT - center_of_curvature_y::BIGINT)
            - (to_x::BIGINT - center_of_curvature_x::BIGINT) * (from_y::BIGINT - center_of_curvature_y::BIGINT)
                AS side
        FROM {{ ref('int__lion') }}
        WHERE curve_flag IN ('L', 'R') AND source_table <> 'altsegmentdata'
    ) AS sides
) AS curves
WHERE curve_flag <> calculated_curve
