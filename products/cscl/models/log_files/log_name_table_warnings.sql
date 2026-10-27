-- Attachment to log.csv, exported as log_name_table_warnings.csv: one row per record
-- whose name prod looks up in one name table but finds only in the other, in prod's
-- wording. Prod logs a warning per record and carries on with the name it found; the same
-- place repeats once per record (SAF-19 and SAF-24 log hundreds), so log.csv gets one
-- count row per id and name (log__name_table_warnings) and the records live here.
--   LION-13/14: a LION feature's preferred B7SC has no principal face code in its own
--     name table (StreetName for centerlines, FeatureName for shoreline, rail, subway and
--     non-street features), but has one in the other (CenterlineExtractFile.cs:2905-2935).
--   LION-15/16: the same for a proto segment's B5SC + LGC1, by its implicit feature class
--     (:3480-3520). Prod never logs LION-15: it compares the un-lowercased table name to a
--     lowercased one (:3501), so a centerline proto re-checks StreetName, finds nothing,
--     and is dropped (CSCL-LOG-LION-19). We log the check it means; int__protosegments
--     takes the face code from either table, so the proto stays in LION.
--   BYTES-05: a centerline record in the LION gdb (base or proto) one of whose AltNames
--     B7SCs (borough + street code + LGC1-4) has no StreetName row but has FeatureName
--     rows (ExtractorClass.cs:2139, :3491). Once per record; prod repeats it per lookup.
--   BYTES-08/09: a LION gdb SAF replicant whose B7SC has no principal name in its own
--     table (StreetName for B-F/O/P and S/V, FeatureName for G/N/X) but has one in the
--     other (:5019, :5077, :5195).
--   SAF-19: an A/B/E/P AltSegmentData row on a centerline whose B5SC + LGC1 has no
--     principal StreetName name but a FeatureName one (SAFExtractClass.cs:1487).
--   SAF-24: a G/N/X CommonPlace in SAF whose B7SC has no principal FeatureName name but a
--     StreetName one (SAFExtractClass.cs:1708).
WITH streetname AS (
    SELECT
        b7sc,
        MAX(NULLIF(TRIM(facecode), '')) AS face_code,
        MAX(NULLIF(TRIM(lookup_key), '')) AS lookup_key
    FROM {{ source('recipe_sources', 'dcp_cscl_streetname') }}
    WHERE principal_flag = 'Y'
    GROUP BY b7sc
),
featurename AS (
    SELECT
        b7sc,
        MAX(NULLIF(TRIM(facecode), '')) AS face_code,
        MAX(NULLIF(TRIM(lookup_key), '')) AS lookup_key
    FROM {{ source('recipe_sources', 'dcp_cscl_featurename') }}
    WHERE principal_flag = 'Y'
    GROUP BY b7sc
),
preferred AS (
    SELECT DISTINCT ON (segmentid)
        segmentid,
        b7sc
    FROM {{ source('recipe_sources', 'dcp_cscl_segment_lgc') }}
    WHERE preferred_lgc_flag = 'Y'
    ORDER BY segmentid, ogc_fid
),
features AS (
    SELECT 'centerline' AS source_table, globalid, segmentid
    FROM {{ source('recipe_sources', 'dcp_cscl_centerline') }}
    UNION ALL
    SELECT 'shoreline', globalid, segmentid
    FROM {{ source('recipe_sources', 'dcp_cscl_shoreline') }}
    UNION ALL
    SELECT 'rail', globalid, segmentid
    FROM {{ source('recipe_sources', 'dcp_cscl_rail') }}
    UNION ALL
    SELECT 'subway', globalid, segmentid
    FROM {{ source('recipe_sources', 'dcp_cscl_subway') }}
    UNION ALL
    SELECT 'nonstreetfeatures', globalid, segmentid
    FROM {{ source('recipe_sources', 'dcp_cscl_nonstreetfeatures') }}
),
protos AS (
    SELECT
        proto.globalid,
        proto.segmentid,
        COALESCE(proto.b5sc, '') || COALESCE(proto.lgc1, '') AS b7sc,
        feature_type_codes.source_feature_class = 'centerline' AS is_centerline
    FROM {{ ref('stg__altsegmentdata_proto') }} AS proto
    LEFT JOIN {{ ref('feature_type_codes') }} AS feature_type_codes
        ON COALESCE(proto.feature_type_code, '') = COALESCE(feature_type_codes.code, '')
),
-- centerline records in the LION gdb (base and proto) and their AltNames B7SCs, the first
-- (lowest LGC slot) that has no StreetName row (as gdb_altnames reads it) but has
-- FeatureName rows
centerline_records AS (
    SELECT lion.*
    FROM {{ ref('int__lion') }} AS lion
    WHERE
        lion.include_in_bytes_lion
        AND (
            lion.source_table = 'centerline'
            OR (
                lion.source_table = 'altsegmentdata'
                AND lion.segmentid IN (SELECT segmentid FROM {{ source('recipe_sources', 'dcp_cscl_centerline') }})
            )
        )
),
altname_b7scs AS (
    SELECT
        centerline_records.globalid,
        centerline_records.source_table,
        centerline_records.lionkey,
        lgc.slot,
        centerline_records.boroughcode || centerline_records.five_digit_street_code || lgc.lgc AS b7sc
    FROM centerline_records
    CROSS JOIN
        LATERAL (
            VALUES
            (1, centerline_records.lgc1),
            (2, centerline_records.lgc2),
            (3, centerline_records.lgc3),
            (4, centerline_records.lgc4)
        ) AS lgc (slot, lgc)
    WHERE lgc.lgc IS NOT NULL
),
any_streetname AS (
    SELECT DISTINCT b7sc FROM {{ source('recipe_sources', 'dcp_cscl_streetname') }}
    WHERE UPPER(TRIM(dcp_flag)) IS DISTINCT FROM 'N'
),
any_featurename AS (
    SELECT
        b7sc,
        MIN(lookup_key) AS lookup_key
    FROM {{ source('recipe_sources', 'dcp_cscl_featurename') }}
    GROUP BY b7sc
),
altname_b7sc_in_featurename AS (
    SELECT DISTINCT ON (altname_b7scs.globalid, altname_b7scs.lionkey)
        altname_b7scs.globalid,
        altname_b7scs.source_table,
        altname_b7scs.lionkey,
        altname_b7scs.b7sc,
        any_featurename.lookup_key AS name
    FROM altname_b7scs
    INNER JOIN any_featurename ON altname_b7scs.b7sc = any_featurename.b7sc
    LEFT JOIN any_streetname ON altname_b7scs.b7sc = any_streetname.b7sc
    WHERE any_streetname.b7sc IS NULL
    ORDER BY altname_b7scs.globalid, altname_b7scs.lionkey, altname_b7scs.slot
),
-- one row per record (globalid), the name found in the other table, and the record's key
found AS (
    SELECT
        CASE WHEN features.source_table = 'centerline' THEN 'CSCL-LOG-LION-13' ELSE 'CSCL-LOG-LION-14' END
            AS log_message_id,
        features.globalid,
        features.source_table AS source_feature_layer,
        'segmentid' AS record_id_type,
        features.segmentid::VARCHAR AS record_id,
        preferred.b7sc,
        CASE WHEN features.source_table = 'centerline' THEN featurename.lookup_key ELSE streetname.lookup_key END
            AS name
    FROM features
    INNER JOIN preferred ON features.segmentid = preferred.segmentid
    LEFT JOIN streetname ON preferred.b7sc = streetname.b7sc
    LEFT JOIN featurename ON preferred.b7sc = featurename.b7sc
    WHERE
        CASE
            WHEN features.source_table = 'centerline'
                THEN streetname.face_code IS NULL AND featurename.face_code IS NOT NULL
            ELSE featurename.face_code IS NULL AND streetname.face_code IS NOT NULL
        END
    UNION ALL
    SELECT
        CASE WHEN protos.is_centerline THEN 'CSCL-LOG-LION-15' ELSE 'CSCL-LOG-LION-16' END,
        protos.globalid,
        'altsegmentdata',
        'segmentid',
        protos.segmentid::VARCHAR,
        protos.b7sc,
        CASE WHEN protos.is_centerline THEN featurename.lookup_key ELSE streetname.lookup_key END
    FROM protos
    LEFT JOIN streetname ON protos.b7sc = streetname.b7sc
    LEFT JOIN featurename ON protos.b7sc = featurename.b7sc
    WHERE
        CASE
            WHEN protos.is_centerline THEN streetname.face_code IS NULL AND featurename.face_code IS NOT NULL
            ELSE featurename.face_code IS NULL AND streetname.face_code IS NOT NULL
        END
    UNION ALL
    SELECT
        'CSCL-LOG-BYTES-05',
        globalid,
        source_table,
        'lionkey',
        lionkey,
        b7sc,
        name
    FROM altname_b7sc_in_featurename
    UNION ALL
    SELECT
        CASE WHEN replicants.specaddr IN ('G', 'N', 'X') THEN 'CSCL-LOG-BYTES-09' ELSE 'CSCL-LOG-BYTES-08' END,
        replicants.saf_globalid,
        replicants.saf_source_table,
        'lionkey',
        replicants.lionkey,
        replicants.b7sc,
        CASE WHEN replicants.specaddr IN ('G', 'N', 'X') THEN streetname.lookup_key ELSE featurename.lookup_key END
    FROM {{ ref('int__saf_replicant_b7sc') }} AS replicants
    LEFT JOIN streetname ON replicants.b7sc = streetname.b7sc
    LEFT JOIN featurename ON replicants.b7sc = featurename.b7sc
    WHERE
        CASE
            WHEN replicants.specaddr IN ('G', 'N', 'X') THEN featurename.b7sc IS NULL AND streetname.b7sc IS NOT NULL
            ELSE streetname.b7sc IS NULL AND featurename.b7sc IS NOT NULL
        END
    UNION ALL
    SELECT
        'CSCL-LOG-SAF-19',
        alt.globalid,
        'altsegmentdata',
        'segmentid',
        alt.segmentid::VARCHAR,
        COALESCE(alt.b5sc, '') || COALESCE(alt.lgc1, ''),
        featurename.lookup_key
    FROM {{ ref('stg__altsegmentdata_saf') }} AS alt
    LEFT JOIN streetname ON (COALESCE(alt.b5sc, '') || COALESCE(alt.lgc1, '')) = streetname.b7sc
    LEFT JOIN featurename ON (COALESCE(alt.b5sc, '') || COALESCE(alt.lgc1, '')) = featurename.b7sc
    WHERE
        alt.saftype IN ('A', 'B', 'E', 'P')
        AND alt.segmentid IN (SELECT segmentid FROM {{ source('recipe_sources', 'dcp_cscl_centerline') }})
        AND streetname.lookup_key IS NULL
        AND featurename.lookup_key IS NOT NULL
    UNION ALL
    SELECT
        'CSCL-LOG-SAF-24',
        gnx.saf_globalid,
        'commonplace',
        'placeid',
        place.placeid::VARCHAR,
        place.b7sc,
        streetname.lookup_key
    FROM (SELECT DISTINCT saf_globalid FROM {{ ref('int__saf_gnx') }}) AS gnx
    INNER JOIN {{ source('recipe_sources', 'dcp_cscl_commonplace_gdb') }} AS place
        ON gnx.saf_globalid = place.globalid
    LEFT JOIN streetname ON place.b7sc = streetname.b7sc
    LEFT JOIN featurename ON place.b7sc = featurename.b7sc
    WHERE featurename.lookup_key IS NULL AND streetname.lookup_key IS NOT NULL
)
SELECT
    'warning' AS log_level,
    'name table warning' AS error_category,
    found.log_message_id,
    log_messages.effect,
    found.globalid,
    found.source_feature_layer,
    found.record_id_type,
    found.record_id,
    found.b7sc,
    found.name,
    '[' || found.log_message_id || '] ' || CASE
        WHEN found.log_message_id = 'CSCL-LOG-SAF-19'
            THEN {{ log_message(
                "[NAME WARNING] The Addressable PlaceName {} is in the FeatureName table, "
                "not in the StreetName table. (AltSegmentData row with a GlobalID = {})",
                ["found.name", "found.globalid"]
            ) }}
        WHEN found.log_message_id = 'CSCL-LOG-SAF-24'
            THEN {{ log_message(
                "[NAME WARNING] The Non-Addressable PlaceName {} is in the StreetName table, "
                "not in the FeatureName table. (CommonPlace row with a PlaceID = {})",
                ["found.name", "found.record_id"]
            ) }}
        WHEN found.log_message_id IN ('CSCL-LOG-LION-14', 'CSCL-LOG-LION-16', 'CSCL-LOG-BYTES-09')
            THEN {{ log_message(
                "[NAME TABLE WARNING] The name for the {} {} with a GlobalID = {} was not "
                "found in the FeatureName table, but was instead found in the StreetName table.",
                [
                    "found.source_feature_layer",
                    "CASE WHEN found.log_message_id = 'CSCL-LOG-LION-16' THEN 'proto segment' ELSE 'feature' END",
                    "found.globalid",
                ]
            ) }}
        ELSE {{ log_message(
            "[NAME TABLE WARNING] The name for the {} {} with a GlobalID = {} was not "
            "found in the StreetName table, but was instead found in the FeatureName table.",
            [
                "found.source_feature_layer",
                "CASE WHEN found.log_message_id = 'CSCL-LOG-LION-15' THEN 'proto segment' ELSE 'feature' END",
                "found.globalid",
            ]
        ) }}
    END AS message
FROM found
LEFT JOIN {{ ref('log_messages') }} AS log_messages ON found.log_message_id = log_messages.id
ORDER BY found.log_message_id, found.name, found.globalid
