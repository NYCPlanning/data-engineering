WITH segments AS (
    SELECT * FROM {{ ref('int__segments') }}
),
nypd AS (
    SELECT * FROM {{ ref('int__segment_nypdbeat') }}
)
SELECT
    'error' AS log_level,
    'segment joined to no nypd beat' AS error_category,
    segments.globalid,
    segments.source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segments.segmentid AS record_id,
    '' AS message
FROM segments
INNER JOIN nypd ON segments.lionkey = nypd.lionkey
WHERE nypd.left_nypd_sector IS NULL AND nypd.right_nypd_sector IS NULL
