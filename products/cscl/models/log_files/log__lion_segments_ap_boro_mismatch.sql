WITH segments AS (
    SELECT * FROM {{ ref("int__segments") }}
),
aps AS (
    SELECT * FROM {{ ref("int__segment_atomicpolygons") }}
)
SELECT
    'neither joined atomic polygon matches segment''s borocode' AS error,
    segments.globalid,
    segments.source_table,
    'segmentid' AS record_id_type,
    segments.segmentid AS record_id,
    FORMAT(
        'Segment from feature layer % with globalid % has boro %, '
        || 'left atomic polygon boro % and right atomic polygon boro %',
        source_table,
        globalid,
        segment_borocode,
        left_borocode,
        right_borocode
    ) AS message
FROM aps
INNER JOIN segments ON aps.globalid = segments.globalid
WHERE
    left_borocode IS DISTINCT FROM segment_borocode AND right_borocode IS DISTINCT FROM segment_borocode
    AND (left_borocode IS NOT NULL OR right_borocode IS NOT NULL) -- to not duplicate rows from other test
