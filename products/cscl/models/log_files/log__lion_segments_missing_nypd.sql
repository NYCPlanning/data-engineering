SELECT
    'segment joined to no nypd beat' AS error,
    globalid,
    source_table,
    'segmentid' AS record_id_type,
    segmentid AS record_id
FROM {{ ref('int__lion') }}
WHERE left_nypd_service_area IS NULL AND right_nypd_service_area IS NULL
