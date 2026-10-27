-- LION gdb values prod changes to fit their field, and logs (ExtractorClass.cs):
--   BYTES-06: an AltNames SName longer than 30 characters is cut to 30 (AddAltNames,
--     :2182, :2256). gdb_altnames cuts it the same way. Thousands of rows, so one count
--     per borough here; the rows are in log_altnames_sname_cut.csv.
--   BYTES-12: a segment with more than 9 records (its own, SAF replicants, coincident
--     segments: int__lion_segcount) gets SegCount 9 (GetCoincidentSegmentCount, :2812).
--     gdb_lion caps it the same way. Once per segment; prod logs it for each record.
WITH segment_features AS (
    SELECT DISTINCT ON (segmentid)
        segmentid,
        globalid,
        source_table
    FROM {{ ref('int__lion') }}
    WHERE include_in_bytes_lion
    ORDER BY segmentid, (source_table = 'altsegmentdata'), globalid
)
SELECT
    'warning' AS log_level,
    'lion gdb sname cut' AS error_category,
    'CSCL-LOG-BYTES-06' AS log_message_id,
    NULL AS globalid,
    'altnames' AS source_feature_layer,
    NULL AS record_id_type,
    NULL AS record_id,
    {{ log_message(
        "{} AltNames rows in {} have an SName longer than 30 characters, cut to 30; "
        "see log_altnames_sname_cut.csv",
        ["COUNT(*)", "borough_name"]
    ) }} AS message,
    COUNT(*) AS record_count
FROM {{ ref('log_altnames_sname_cut') }}
GROUP BY borough_name
UNION ALL
SELECT
    'warning' AS log_level,
    'lion gdb segcount capped' AS error_category,
    'CSCL-LOG-BYTES-12' AS log_message_id,
    segment_features.globalid,
    segment_features.source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segcount.segmentid::VARCHAR AS record_id,
    {{ log_message(
        "[COINCIDENT SEGMENT COUNT WARNING] A coincident segment count = {} was found for "
        "the {} feature with a SEGMENTID = {}, however the output only supports 1 character.  "
        "Setting the value = 9.",
        ["segcount.n_records", "segment_features.source_table", "segcount.segmentid"]
    ) }} AS message,
    NULL AS record_count
FROM {{ ref('int__lion_segcount') }} AS segcount
INNER JOIN segment_features ON segcount.segmentid = segment_features.segmentid
WHERE segcount.n_records > 9
