SELECT
    'segment missing node' AS error,
    globalid,
    source_table,
    'segmentid' AS record_id_type,
    segmentid AS record_id
FROM {{ ref('int__lion') }}
WHERE from_nodeid IS NULL OR to_nodeid IS NULL
