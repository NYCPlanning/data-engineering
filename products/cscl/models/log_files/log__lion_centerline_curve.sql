SELECT
    'warning' AS log_level,
    'center of curvature' AS error_category,
    'CSCL-LOG-LION-07' AS log_message_id,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    -- prod also prints the angle, and falls back to the segment midpoint; we do neither
    {{ log_message(
        "[INVALID ANGLE & RADIUS FOUND] An invalid angle & radius value was found while "
        "calculating the segment curvature for the {} feature with a GlobalID = {}.  "
        "Radius = {}, Calculated X = {}, Calculated Y = {}.",
        ["source_table", "globalid", "center_of_curvature_radius", "center_of_curvature_x", "center_of_curvature_y"]
    ) }} AS message
FROM {{ ref('int__lion') }}
WHERE
    center_of_curvature_x NOT BETWEEN 0 AND 9999999
    OR center_of_curvature_y NOT BETWEEN 0 AND 9999999
UNION ALL
-- CURVE is L or R but the start, middle and end points are collinear: no circle, so the
-- center of curvature falls back to the segment midpoint, as in prod. Once per
-- centerline: proto segments copy their centerline's curvature.
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
    AND curve.center_is_midpoint
