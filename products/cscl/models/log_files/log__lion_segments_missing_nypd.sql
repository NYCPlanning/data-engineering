SELECT
    'segment joined to no nypd beat' AS error,
    segments.globalid,
    segments.source_table,
    'segmentid' AS record_id_type,
    segments.segmentid AS record_id
FROM {{ ref('int__segments' ) }} AS segments
INNER JOIN {{ ref('int__segment_nypdbeat') }} AS beat ON segments.lionkey = beat.lionkey
WHERE beat.left_nypd_sector IS NULL AND beat.right_nypd_sector IS NULL
