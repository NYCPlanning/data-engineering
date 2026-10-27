-- District GDB fields left blank because a lookup table has no row for the feature's code
-- (GDBExtractorClass.cs:1520-1720). Read off our district layers, so each row is a field
-- our output leaves blank. Prod logs again for each macro field that reads the lookup;
-- we log once per feature and lookup.
--   GDB-03: nynta2020's CDTA (prod finds it spatially, $cdtacode2020) has no CDTAEquiv2020
--     row, so CDTAName is blank.
--   GDB-04: no NeighborhoodPUMACodes row, so NTAName is blank (nyct2010, nyct2010wi,
--     nynta2010). Prod looks up the tract's NEIGHBORHOOD_CODE; gdb_nyct2010 matches on
--     borough and tract.
--   GDB-05: the NeighborhoodPUMACodes row has a blank FIPS_CODE. Prod blanks nynta2010's
--     CountyFIPS; ours comes from the borough, so it isn't blank.
--   GDB-06: the NTA code has no NTAEquiv2020 row, so NTAName (and nynta2020's NTAAbbrev and
--     NTAType) are blank (nyct2020, nyct2020wi, nynta2020).
--   GDB-07: the CDTA code has no CDTAEquiv2020 row, so CDTAName (and nycdta2020's
--     CDTAType) are blank (nyct2020, nyct2020wi, nycdta2020).
WITH npc AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_neighborhoodpumacodes') }}
),
tract2010 AS (
    SELECT * FROM {{ ref('stg__censustract2010') }}
),
missing AS (
    SELECT
        'CSCL-LOG-GDB-03' AS log_message_id,
        'nynta2020' AS layer,
        "NTA2020" AS feature_code,
        'CDTAEquiv2020' AS lookup_table,
        'CDTA_CODE' AS lookup_field,
        "CDTA2020" AS lookup_code
    FROM {{ ref('gdb_nynta2020') }}
    WHERE "CDTA2020" IS NOT NULL AND "CDTAName" IS NULL
    UNION ALL
    SELECT
        'CSCL-LOG-GDB-04' AS log_message_id,
        'nyct2010' AS layer,
        ct."BoroCT2010" AS feature_code,
        'NeighborhoodPUMACodes' AS lookup_table,
        'NEIGHBORHOOD_CODE' AS lookup_field,
        tract.neighborhood_code AS lookup_code
    FROM {{ ref('gdb_nyct2010') }} AS ct
    LEFT JOIN tract2010 AS tract ON ct."BoroCT2010" = tract.boroct
    WHERE ct."NTAName" IS NULL
    UNION ALL
    SELECT
        'CSCL-LOG-GDB-04' AS log_message_id,
        'nyct2010wi' AS layer,
        ct."BoroCT2010" AS feature_code,
        'NeighborhoodPUMACodes' AS lookup_table,
        'NEIGHBORHOOD_CODE' AS lookup_field,
        tract.neighborhood_code AS lookup_code
    FROM {{ ref('gdb_nyct2010wi') }} AS ct
    LEFT JOIN tract2010 AS tract ON ct."BoroCT2010" = tract.boroct
    WHERE ct."NTAName" IS NULL
    UNION ALL
    SELECT
        'CSCL-LOG-GDB-04' AS log_message_id,
        'nynta2010' AS layer,
        "NTACode" AS feature_code,
        'NeighborhoodPUMACodes' AS lookup_table,
        'NEIGHBORHOOD_CODE' AS lookup_field,
        "NTACode" AS lookup_code
    FROM {{ ref('gdb_nynta2010') }}
    WHERE "NTAName" IS NULL
    UNION ALL
    SELECT DISTINCT
        'CSCL-LOG-GDB-05' AS log_message_id,
        'nynta2010' AS layer,
        nta."NTACode" AS feature_code,
        'NeighborhoodPUMACodes' AS lookup_table,
        'NEIGHBORHOOD_CODE' AS lookup_field,
        nta."NTACode" AS lookup_code
    FROM {{ ref('gdb_nynta2010') }} AS nta
    INNER JOIN npc ON nta."NTACode" = npc.neighborhood_code
    WHERE TRIM(COALESCE(npc.fips_code, '')) = ''
    UNION ALL
    SELECT
        'CSCL-LOG-GDB-06' AS log_message_id,
        'nyct2020' AS layer,
        "BoroCT2020" AS feature_code,
        'NTAEquiv2020' AS lookup_table,
        'NTA_CODE' AS lookup_field,
        "NTA2020" AS lookup_code
    FROM {{ ref('gdb_nyct2020') }}
    WHERE "NTAName" IS NULL
    UNION ALL
    SELECT
        'CSCL-LOG-GDB-06' AS log_message_id,
        'nyct2020wi' AS layer,
        "BoroCT2020" AS feature_code,
        'NTAEquiv2020' AS lookup_table,
        'NTA_CODE' AS lookup_field,
        "NTA2020" AS lookup_code
    FROM {{ ref('gdb_nyct2020wi') }}
    WHERE "NTAName" IS NULL
    UNION ALL
    SELECT
        'CSCL-LOG-GDB-06' AS log_message_id,
        'nynta2020' AS layer,
        "NTA2020" AS feature_code,
        'NTAEquiv2020' AS lookup_table,
        'NTA_CODE' AS lookup_field,
        "NTA2020" AS lookup_code
    FROM {{ ref('gdb_nynta2020') }}
    WHERE "NTAName" IS NULL
    UNION ALL
    SELECT
        'CSCL-LOG-GDB-07' AS log_message_id,
        'nyct2020' AS layer,
        "BoroCT2020" AS feature_code,
        'CDTAEquiv2020' AS lookup_table,
        'CDTA_CODE' AS lookup_field,
        "CDTA2020" AS lookup_code
    FROM {{ ref('gdb_nyct2020') }}
    WHERE "CDTA2020" IS NOT NULL AND "CDTANAME" IS NULL
    UNION ALL
    SELECT
        'CSCL-LOG-GDB-07' AS log_message_id,
        'nyct2020wi' AS layer,
        "BoroCT2020" AS feature_code,
        'CDTAEquiv2020' AS lookup_table,
        'CDTA_CODE' AS lookup_field,
        "CDTA2020" AS lookup_code
    FROM {{ ref('gdb_nyct2020wi') }}
    WHERE "CDTA2020" IS NOT NULL AND "CDTANAME" IS NULL
    UNION ALL
    SELECT
        'CSCL-LOG-GDB-07' AS log_message_id,
        'nycdta2020' AS layer,
        "CDTA2020" AS feature_code,
        'CDTAEquiv2020' AS lookup_table,
        'CDTA_CODE' AS lookup_field,
        "CDTA2020" AS lookup_code
    FROM {{ ref('gdb_nycdta2020') }}
    WHERE "CDTAName" IS NULL
)
SELECT
    'error' AS log_level,
    'district lookup missing' AS error_category,
    log_message_id,
    NULL AS globalid,
    layer AS source_feature_layer,
    'district' AS record_id_type,
    feature_code AS record_id,
    CASE log_message_id
        WHEN 'CSCL-LOG-GDB-05'
            THEN {{ log_message(
                "Unable to locate a FIPS_CODE value in the {} using the query {} = '{}.",
                ["lookup_table", "lookup_field", "lookup_code"]
            ) }}
        ELSE {{ log_message(
            "Unable to locate a matching in the {} using the query {} = '{}.",
            ["lookup_table", "lookup_field", "lookup_code"]
        ) }}
    END AS message
FROM missing
