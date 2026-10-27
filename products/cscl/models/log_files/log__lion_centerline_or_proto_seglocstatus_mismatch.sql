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
    CASE source_table
        WHEN 'centerline' THEN 'CSCL-LOG-LION-01'
        WHEN 'altsegmentdata' THEN 'CSCL-LOG-LION-02'
    END AS log_message_id,
    'warned' AS effect,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    -- prod's LION-01 wording ends with start/end node counts and OIDs, which we don't compute
    CASE source_table
        WHEN 'centerline'
            THEN FORMAT(
                '[CSCL-LOG-LION-01] The {} feature with a GlobalID = {} has a SEGLOCSTATUS value '
                || 'that is different than what was found using the LION ETL rules.  '
                || 'Feature = ''{}'', ETL Rules = ''{}''.',
                source_table,
                COALESCE(globalid::VARCHAR, ''),
                COALESCE(source_segment_locational_status::VARCHAR, ''),
                COALESCE(segment_locational_status::VARCHAR, '')
            )
        WHEN 'altsegmentdata'
            THEN FORMAT(
                '[CSCL-LOG-LION-02] The proto segment record a GlobalID = {} has a SEGLOCSTATUS '
                || 'value that is different than what was found using the LION ETL rules.  '
                || 'Proto Segment Row = ''{}'', ETL Rules = ''{}''.',
                COALESCE(globalid::VARCHAR, ''),
                COALESCE(source_segment_locational_status::VARCHAR, ''),
                COALESCE(segment_locational_status::VARCHAR, '')
            )
    END AS message
FROM joined
WHERE segment_locational_status IS DISTINCT FROM source_segment_locational_status
