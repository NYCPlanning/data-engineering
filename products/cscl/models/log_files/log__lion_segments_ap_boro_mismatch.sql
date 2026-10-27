WITH segments AS (
    SELECT * FROM {{ ref("int__segments") }}
),
aps AS (
    SELECT * FROM {{ ref("int__segment_atomicpolygons") }}
)
SELECT
    'error' AS log_level,
    'neither joined atomic polygon matches segment''s borocode' AS error_category,
    -- prod checks centerlines (LION-03) and shorelines (LION-04); we check every segment
    CASE WHEN segments.source_table = 'shoreline' THEN 'CSCL-LOG-LION-04' ELSE 'CSCL-LOG-LION-03' END
        AS log_message_id,
    segments.globalid,
    segments.source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segments.segmentid AS record_id,
    {{ log_message(
        "Neither of the AtomicPolygon borough matches the segment borough for the {} feature "
        "with a GlobalID = {}. Left Borough = '{}', Right Borough = '{}' and Segment Borough = '{}'.",
        ["segments.source_table", "segments.globalid", "aps.left_borocode", "aps.right_borocode", "aps.segment_borocode"]
    ) }} AS message
FROM aps
INNER JOIN segments ON aps.globalid = segments.globalid
WHERE
    left_borocode IS DISTINCT FROM segment_borocode
    AND right_borocode IS DISTINCT FROM segment_borocode
    AND (left_borocode IS NOT NULL OR right_borocode IS NOT NULL) -- to not duplicate rows from other test
