SELECT
    'warning' AS log_level,
    'center of curvature' AS error_category,
    'CSCL-LOG-LION-07' AS log_message_id,
    'warned' AS effect,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    -- prod also prints the angle, and falls back to the segment midpoint; we do neither
    FORMAT(
        '[CSCL-LOG-LION-07] [INVALID ANGLE & RADIUS FOUND] An invalid angle & radius value was '
        || 'found while calculating the segment curvature for the {} feature with a GlobalID = {}.  '
        || 'Radius = {}, Calculated X = {}, Calculated Y = {}.',
        COALESCE(source_table::VARCHAR, ''),
        COALESCE(globalid::VARCHAR, ''),
        COALESCE(center_of_curvature_radius::VARCHAR, ''),
        COALESCE(center_of_curvature_x::VARCHAR, ''),
        COALESCE(center_of_curvature_y::VARCHAR, '')
    ) AS message
FROM {{ ref('int__lion') }}
WHERE
    center_of_curvature_x NOT BETWEEN 0 AND 9999999
    OR center_of_curvature_y NOT BETWEEN 0 AND 9999999
