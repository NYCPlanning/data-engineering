-- RPL records prod writes with a field left blank (RPLExtractClass.cs:88-207), for rows
-- whose generic and roadbed centerlines both exist (the others are CSCL-LOG-RPL-05/06):
--   RPL-01..04: no Node at the roadbed's from point (RPL8), the generic's from point
--     (RPL9), the roadbed's to point (RPL10) or the generic's to point (RPL11). int__rpl
--     leaves the node id blank too.
--   RPL-08: FROM_/TO_NODE_LEVEL_COINCIDENT_RB is a number other than 1-26 or 99
--     (GetLevelCode, :370). Prod leaves the level code blank; int__rpl doesn't (it writes
--     chr(64 + n), e.g. '^' for 30), and writes no '*' for 99.
WITH source_rpl AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_roadbed_pointer_list') }}
),
centerline AS (
    SELECT DISTINCT segmentid
    FROM {{ source('recipe_sources', 'dcp_cscl_centerline') }}
    WHERE geom IS NOT NULL AND NOT ST_ISEMPTY(geom)
),
rpl AS (
    SELECT * FROM {{ ref('int__rpl') }}
    WHERE
        generic_segmentid IN (SELECT segmentid FROM centerline)
        AND roadbed_segmentid IN (SELECT segmentid FROM centerline)
),
missing_nodes AS (
    SELECT
        rpl.globalid,
        rpl.generic_segmentid,
        rpl.roadbed_segmentid,
        node.log_message_id,
        node.field_name,
        node.which
    FROM rpl
    CROSS JOIN
        LATERAL (
            VALUES
            ('CSCL-LOG-RPL-01', 'RPL8', 'roadbed from', rpl.from_nodeid_of_roadbed_segment),
            ('CSCL-LOG-RPL-02', 'RPL9', 'generic from', rpl.from_nodeid_of_generic_segment),
            ('CSCL-LOG-RPL-03', 'RPL10', 'roadbed to', rpl.to_nodeid_of_roadbed_segment),
            ('CSCL-LOG-RPL-04', 'RPL11', 'generic to', rpl.to_nodeid_of_generic_segment)
        ) AS node (log_message_id, field_name, which, nodeid)
    WHERE node.nodeid IS NULL
),
level_codes AS (
    SELECT
        source_rpl.globalid,
        source_rpl.roadbed_segmentid,
        level.field_name,
        level.level_code
    FROM source_rpl
    INNER JOIN rpl ON source_rpl.globalid = rpl.globalid
    CROSS JOIN
        LATERAL (
            VALUES
            ('FROM_NODE_LEVEL_COINCIDENT_RB', source_rpl.from_node_level_coincident_rb),
            ('TO_NODE_LEVEL_COINCIDENT_RB', source_rpl.to_node_level_coincident_rb)
        ) AS level (field_name, level_code)
    WHERE NOT (level.level_code BETWEEN 1 AND 26 OR level.level_code = 99)
)
SELECT
    'error' AS log_level,
    'rpl node missing' AS error_category,
    log_message_id,
    globalid,
    'roadbed_pointer_list' AS source_feature_layer,
    'roadbed_segmentid' AS record_id_type,
    roadbed_segmentid AS record_id,
    {{ log_message(
        "{} missing in [RPL] record from [RoadbedPointerList] table with "
        "GENERIC_SEGMENTID = [{}] and ROADBED_SEGMENTID = [{}] as {} node feature is NULL",
        ["field_name", "generic_segmentid", "roadbed_segmentid", "which"]
    ) }} AS message
FROM missing_nodes
UNION ALL
SELECT
    'warning' AS log_level,
    'invalid level code' AS error_category,
    'CSCL-LOG-RPL-08' AS log_message_id,
    globalid,
    'roadbed_pointer_list' AS source_feature_layer,
    'roadbed_segmentid' AS record_id_type,
    roadbed_segmentid AS record_id,
    {{ log_message(
        "[NO MATCHING LEVEL CODE] The {} field in the RoadbedPointerList row with a "
        "GlobalID = {} does not appear to have a valid value ({}).",
        ["field_name", "globalid", "level_code"]
    ) }} AS message
FROM level_codes
