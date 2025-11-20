WITH lion AS (
    SELECT * FROM {{ ref("int__lion") }}
),
centerline AS (
    SELECT
        segmentid,
        boroughcode
    FROM {{ ref("stg__centerline") }}
),
proto AS (
    SELECT * FROM {{ ref("stg__altsegmentdata_proto") }}
),
lion_joined AS (
    SELECT
        lion.lionkey,
        lion.globalid,
        lion.segmentid,
        lion.boroughcode,
        CASE
            WHEN lion.source_table = 'centerline' THEN centerline.boroughcode
            WHEN lion.source_table = 'altsegmentdata' THEN proto.borough
        END AS source_boroughcode,
        lion.source_table
    FROM lion
    LEFT JOIN centerline ON lion.source_table = 'centerline' AND lion.segmentid = centerline.segmentid
    LEFT JOIN proto ON lion.source_table = 'altsegmentdata' AND lion.globalid = proto.globalid
    WHERE lion.source_table IN ('centerline', 'altsegmentdata')
)
SELECT
    'error' AS log_level,
    'borough mismatch' AS error_category,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    lion_joined.segmentid AS record_id,
    FORMAT(
        'The borough for the %s feature with an OID = "%s" (Borough = %s) does not '
        || 'match the borough currently being extracted (Borough = %s).',
        source_table,
        globalid,
        source_boroughcode,
        lion_joined.boroughcode
    ) AS message
FROM lion_joined
INNER JOIN centerline ON lion_joined.segmentid = centerline.segmentid
WHERE centerline.boroughcode IS DISTINCT FROM lion_joined.boroughcode
