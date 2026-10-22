-- Prod (LIONExtractorClass.PreExportCheckFeature) compares a feature's own borough (the
-- centerline's BoroughCode, or the proto segment's BOROUGH; blank falls back to a derived
-- borough) to the borough of the LION file it's being written to. Here: the source row's borough vs the borough of the LION
-- row built from it. Coincident borough-boundary protos (ALT_SEGDATA_TYPE 'B') carry the
-- other borough from their centerline by design, so comparing a proto to its centerline
-- would flag every one of them.
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
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    FORMAT(
        'The borough for the {} feature with an OID = "{}" (Borough = {}) does not '
        || 'match the borough currently being extracted (Borough = {}).',
        COALESCE(source_table::VARCHAR, ''),
        COALESCE(globalid::VARCHAR, ''),
        COALESCE(source_boroughcode::VARCHAR, ''),
        COALESCE(boroughcode::VARCHAR, '')
    ) AS message
FROM lion_joined
-- a blank source borough is derived downstream (prod falls back the same way), not a mismatch
WHERE
    TRY_CAST(source_boroughcode AS INT) IS NOT NULL
    AND TRY_CAST(source_boroughcode AS INT) IS DISTINCT FROM TRY_CAST(boroughcode AS INT)
