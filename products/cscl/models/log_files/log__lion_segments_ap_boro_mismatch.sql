WITH segments AS (
    SELECT * FROM {{ ref("int__segments") }}
),
aps AS (
    SELECT * FROM {{ ref("int__segment_atomicpolygons") }}
)
SELECT
    'error' AS log_level,
    'neither joined atomic polygon matches segment''s borocode' AS error_category,
    segments.globalid,
    segments.source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segments.segmentid AS record_id,
    FORMAT(
        'Neither of the AtomicPolygon borough matches the segment borough for '
        || 'the %s feature with an OID = %s. Segment has boro ''%s'', '
        || 'left atomic polygon with atomicid ''%s'' has boro ''%s'', '
        || 'and right atomic polygon with atomicid ''%s'' has boro ''%s''.',
        segments.source_table,
        segments.globalid,
        aps.segment_borocode,
        aps.left_atomicid,
        aps.left_borocode,
        aps.right_atomicid,
        aps.right_borocode
    ) AS message
FROM aps
INNER JOIN segments ON aps.globalid = segments.globalid
WHERE
    left_borocode IS DISTINCT FROM segment_borocode
    AND right_borocode IS DISTINCT FROM segment_borocode
    AND (left_borocode IS NOT NULL OR right_borocode IS NOT NULL) -- to not duplicate rows from other test
