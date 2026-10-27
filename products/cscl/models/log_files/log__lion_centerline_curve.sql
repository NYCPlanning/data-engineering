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
-- CURVE is L or R but the start, middle and end points are collinear: no circle, so no
-- center of curvature. Prod falls back to the segment midpoint; we leave it blank. Once
-- per centerline: proto segments copy their centerline's curvature.
SELECT
    'warning' AS log_level,
    'center of curvature' AS error_category,
    'CSCL-LOG-LION-08' AS log_message_id,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    {{ log_message(
        "[NO CENTER POINT FOUND] Unable to locate a valid center point while calculating "
        "the segment curvature for the {} feature with a GlobalID = {}.",
        ["source_table", "globalid"]
    ) }} AS message
FROM {{ ref('int__lion') }}
WHERE
    source_table = 'centerline'
    AND curve_flag IN ('L', 'R')
    AND center_of_curvature_x IS NULL
