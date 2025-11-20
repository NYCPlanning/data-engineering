SELECT
    'protosegment without geometry-modeled segment' AS error,
    globalid,
    source_table,
    'segmentid' AS record_id_type,
    segmentid AS record_id
FROM {{ ref('int__protosegments') }}
WHERE geom IS NULL
