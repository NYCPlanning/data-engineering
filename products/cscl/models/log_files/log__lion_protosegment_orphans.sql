SELECT
    'protosegment without geometry-modeled segment' AS error,
    globalid,
    source_table,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    FORMAT(
        'Protosegment with globalid % and segmentid % has no corresponding geometry-modeled segment.',
        globalid,
        segmentid
    ) AS message
FROM {{ ref('int__protosegments') }}
WHERE geom IS NULL
