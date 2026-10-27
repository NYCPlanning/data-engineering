-- LION gdb values prod changes to fit their field, and logs (ExtractorClass.cs):
--   BYTES-06: an AltNames SName longer than 30 characters is cut to 30 (AddAltNames,
--     :2182, :2256). gdb_altnames cuts it the same way. One row per AltNames row cut.
--   BYTES-12: a segment with more than 9 records (its own, SAF replicants, coincident
--     segments: int__lion_segcount) gets SegCount 9 (GetCoincidentSegmentCount, :2812).
--     gdb_lion caps it the same way. Once per segment; prod logs it for each record.
WITH street_names AS (
    -- gdb_altnames' SName before the cut: the street name, or with a pre or post
    -- modifier the components joined, each followed by a space
    SELECT
        lookup_key AS street,
        CASE
            WHEN NULLIF(pre_modifier, '') IS NULL AND NULLIF(post_modifier, '') IS NULL
                THEN COALESCE(street_name, '')
            WHEN NULLIF(post_modifier, '') IS NULL
                THEN CONCAT_WS(
                    ' ', pre_modifier, NULLIF(pre_directional, ''), NULLIF(pre_type, ''), COALESCE(street_name, '')
                ) || ' '
            WHEN NULLIF(pre_modifier, '') IS NULL
                THEN CONCAT_WS(
                    ' ', COALESCE(street_name, ''), NULLIF(post_directional, ''), NULLIF(post_type, ''), post_modifier
                ) || ' '
            ELSE CONCAT_WS(
                ' ', pre_modifier, NULLIF(pre_directional, ''), NULLIF(pre_type, ''), COALESCE(street_name, ''),
                NULLIF(post_directional, ''), NULLIF(post_type, ''), post_modifier
            ) || ' '
        END AS sname
    FROM {{ source('recipe_sources', 'dcp_cscl_streetname') }}
    WHERE UPPER(TRIM(dcp_flag)) IS DISTINCT FROM 'N'
),
long_names AS (
    SELECT DISTINCT
        street,
        sname
    FROM (
        SELECT * FROM street_names
        UNION ALL
        SELECT
            lookup_key AS street,
            lookup_key AS sname
        FROM {{ source('recipe_sources', 'dcp_cscl_featurename') }}
    ) AS names
    WHERE LENGTH(sname) > 30
),
segment_features AS (
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
    'join_id' AS record_id_type,
    altnames."Join_ID" AS record_id,
    {{ log_message(
        "The SName value [{}] for the AltNames row with a Join_ID = {} is too long.  "
        "Expected 30 characters but got {}.  Truncating to 30 characters.",
        ["long_names.sname", 'altnames."Join_ID"', "LENGTH(long_names.sname)"]
    ) }} AS message
FROM {{ ref('gdb_altnames') }} AS altnames
INNER JOIN long_names
    ON altnames."Street" = long_names.street AND altnames."SName" = LEFT(long_names.sname, 30)
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
    ) }} AS message
FROM {{ ref('int__lion_segcount') }} AS segcount
INNER JOIN segment_features ON segcount.segmentid = segment_features.segmentid
WHERE segcount.n_records > 9
