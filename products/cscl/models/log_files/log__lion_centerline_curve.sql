SELECT
    'warning' AS log_level,
    'center of curvature' AS error_category,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    FORMAT(
        'An invalid center of curvature was calculated for the {} feature with an OID = {}. '
        || 'Calculated X = {}, Calculated Y = {}.',
        coalesce(source_table::VARCHAR, ''),
        coalesce(globalid::VARCHAR, ''),
        coalesce(center_of_curvature_x::VARCHAR, ''),
        coalesce(center_of_curvature_y::VARCHAR, '')
    ) AS message
FROM {{ ref('int__lion') }}
WHERE
    center_of_curvature_x NOT BETWEEN 0 AND 9999999
    OR center_of_curvature_y NOT BETWEEN 0 AND 9999999
