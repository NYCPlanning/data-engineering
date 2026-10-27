WITH segments AS (
    SELECT * FROM {{ ref('int__segments') }}
),
nypd AS (
    SELECT * FROM {{ ref('int__segment_nypdbeat') }}
)
SELECT
    'error' AS log_level,
    'segment joined to no nypd beat' AS error_category,
    'CSCL-LOG-OURS-03' AS log_message_id,
    segments.globalid,
    segments.source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segments.segmentid AS record_id,
    {{ log_message(
        "Neither side of the {} feature with a GlobalID = {} falls in an NYPD beat.",
        ["segments.source_table", "segments.globalid"]
    ) }} AS message
FROM segments
INNER JOIN nypd ON segments.lionkey = nypd.lionkey
WHERE nypd.left_nypd_sector IS NULL AND nypd.right_nypd_sector IS NULL
