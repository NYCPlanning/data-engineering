-- Attachment to log.csv, exported as log_altnames_sname_cut.csv: one row per LION gdb
-- AltNames row whose SName is longer than 30 characters, which prod cuts to 30 and logs
-- (CSCL-LOG-BYTES-06, ExtractorClass.cs AddAltNames :2182, :2256); gdb_altnames cuts it
-- the same way. There are thousands, so log.csv carries one count per borough (the
-- Join_ID's first character; log__gdb_lion_value_changed) and the records live here, in
-- prod's wording.
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
)
SELECT
    'warning' AS log_level,
    'lion gdb sname cut' AS error_category,
    'CSCL-LOG-BYTES-06' AS log_message_id,
    log_messages.effect,
    NULL AS globalid,
    'altnames' AS source_feature_layer,
    'join_id' AS record_id_type,
    altnames."Join_ID" AS record_id,
    CASE LEFT(altnames."Join_ID", 1)
        WHEN '1' THEN 'Manhattan'
        WHEN '2' THEN 'Bronx'
        WHEN '3' THEN 'Brooklyn'
        WHEN '4' THEN 'Queens'
        WHEN '5' THEN 'StatenIsland'
    END AS borough_name,
    altnames."Street" AS street,
    long_names.sname,
    '[CSCL-LOG-BYTES-06] ' || {{ log_message(
        "The SName value [{}] for the AltNames row with a Join_ID = {} is too long.  "
        "Expected 30 characters but got {}.  Truncating to 30 characters.",
        ["long_names.sname", 'altnames."Join_ID"', "LENGTH(long_names.sname)"]
    ) }} AS message
FROM {{ ref('gdb_altnames') }} AS altnames
INNER JOIN long_names
    ON altnames."Street" = long_names.street AND altnames."SName" = LEFT(long_names.sname, 30)
LEFT JOIN {{ ref('log_messages') }} AS log_messages ON log_messages.id = 'CSCL-LOG-BYTES-06'
ORDER BY altnames."Join_ID", altnames."Street"
