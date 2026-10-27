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
