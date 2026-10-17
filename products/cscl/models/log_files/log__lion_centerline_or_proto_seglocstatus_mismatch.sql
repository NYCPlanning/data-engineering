WITH lion AS (
    SELECT * FROM {{ ref("int__lion") }}
),
centerline AS (
    SELECT * FROM {{ ref("stg__centerline") }}
),
proto AS (
    SELECT * FROM {{ ref("stg__altsegmentdata_proto") }}
),
joined AS (
    SELECT
        lion.lionkey,
        lion.globalid,
        lion.segmentid,
        lion.segment_locational_status,
        CASE
            WHEN lion.source_table = 'centerline' THEN centerline.seglocstatus
            WHEN lion.source_table = 'altsegmentdata' THEN proto.seglocstatus
        END AS source_segment_locational_status,
        lion.source_table
    FROM lion
    -- one-sided conditions folded into the key (a one-sided ON predicate nested-loops in DuckDB)
    LEFT JOIN centerline
        ON (CASE WHEN lion.source_table = 'centerline' THEN lion.segmentid END) = centerline.segmentid
    LEFT JOIN proto
        ON (CASE WHEN lion.source_table = 'altsegmentdata' THEN lion.globalid END) = proto.globalid
    WHERE lion.source_table IN ('centerline', 'altsegmentdata')
)
SELECT
    'error' AS log_level,
    'seglocstatus mismatch' AS error_category,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    FORMAT(
        'The {} feature with an OID = {} has a SEGLOCSTATUS value that is different than '
        || 'what was found using the LION ETL rules.  Feature = ''{}'', ETL Rules = ''{}''.',
        COALESCE(source_table::VARCHAR, ''),
        COALESCE(globalid::VARCHAR, ''),
        COALESCE(source_segment_locational_status::VARCHAR, ''),
        COALESCE(segment_locational_status::VARCHAR, '')
    ) AS message
FROM joined
WHERE segment_locational_status IS DISTINCT FROM source_segment_locational_status
