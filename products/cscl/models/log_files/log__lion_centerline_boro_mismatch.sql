-- LION rows whose borough differs from their source feature's own borough (the
-- centerline's boroughcode, or the proto segment's borough). Prod has a message for a
-- similar check (CSCL-LOG-LION-20) but never reaches it, so this logs under its own id.
-- Coincident borough-boundary protos (ALT_SEGDATA_TYPE 'B') carry the other borough from
-- their centerline by design, so comparing a proto to its centerline would flag every one
-- of them.
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
    -- one-sided conditions folded into the key (a one-sided ON predicate nested-loops in DuckDB)
    LEFT JOIN centerline
        ON (CASE WHEN lion.source_table = 'centerline' THEN lion.segmentid END) = centerline.segmentid
    LEFT JOIN proto
        ON (CASE WHEN lion.source_table = 'altsegmentdata' THEN lion.globalid END) = proto.globalid
    WHERE lion.source_table IN ('centerline', 'altsegmentdata')
)
SELECT
    'error' AS log_level,
    'borough mismatch' AS error_category,
    'CSCL-LOG-OURS-04' AS log_message_id,
    'warned' AS effect,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    FORMAT(
        '[CSCL-LOG-OURS-04] The LION record built from the {} feature with a GlobalID = {} has '
        || 'Borough = {}, but the feature''s own borough is {}.',
        COALESCE(source_table::VARCHAR, ''),
        COALESCE(globalid::VARCHAR, ''),
        COALESCE(boroughcode::VARCHAR, ''),
        COALESCE(source_boroughcode::VARCHAR, '')
    ) AS message
FROM lion_joined
-- a blank source borough is derived downstream (prod falls back the same way), not a mismatch
WHERE
    TRY_CAST(source_boroughcode AS INT) IS NOT NULL
    AND TRY_CAST(source_boroughcode AS INT) IS DISTINCT FROM TRY_CAST(boroughcode AS INT)
