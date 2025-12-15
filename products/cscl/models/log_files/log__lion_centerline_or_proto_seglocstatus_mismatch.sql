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
    LEFT JOIN centerline ON lion.source_table = 'centerline' AND lion.segmentid = centerline.segmentid
    LEFT JOIN proto ON lion.source_table = 'altsegmentdata' AND lion.globalid = proto.globalid
    WHERE lion.source_table IN ('centerline', 'altsegmentdata')
)
SELECT
    'seglocstatus mismatch' AS error_category,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    FORMAT(
        'Segment from feature layer "%s" with globalid "%s" has calculated seglocstatus "%s" and source field seglocstatus "%s"',
        source_table,
        globalid,
        segment_locational_status,
        source_segment_locational_status
    ) AS message
FROM joined
WHERE segment_locational_status IS DISTINCT FROM source_segment_locational_status
