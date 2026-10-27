-- LION rows whose borough differs from their source feature's own borough (the
-- centerline's boroughcode, or the proto segment's borough): the record goes to another
-- borough's LION file than the feature's (the files are split on the record's borough).
-- That is the check prod means by CSCL-LOG-LION-20 (LIONExtractorClass.cs:461-471), but
-- prod's version never fires: it compares the record's borough to the file's, and the
-- check before it (:436-439) has already skipped every record of another borough.
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
    'CSCL-LOG-LION-20' AS log_message_id,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    {{ log_message(
        "The borough for the {} feature with a GlobalID = {} (Borough = {}) does not match "
        "the borough of the LION file it is written to (Borough = {}).",
        ["source_table", "globalid", "source_boroughcode", "boroughcode"]
    ) }} AS message
FROM lion_joined
-- a blank source borough is derived downstream (prod falls back the same way), not a mismatch
WHERE
    TRY_CAST(source_boroughcode AS INT) IS NOT NULL
    AND TRY_CAST(source_boroughcode AS INT) IS DISTINCT FROM TRY_CAST(boroughcode AS INT)
