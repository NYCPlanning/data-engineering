WITH saf AS (
    SELECT * FROM {{ ref('int__saf_segments') }}
),
address_points AS (
    SELECT * FROM {{ ref('int__address_points') }}
),
feature_names AS (
    SELECT * FROM {{ ref('stg__facecode_and_featurename_principal') }}
),
-- LGC1 is the preferred LGC, LGC2-4 the others in Segment_LGC row order (legacy
-- GetSegmentLGC reads an unsorted cursor, i.e. ObjectID order; LION sorts by value instead).
-- BOE_LGC points at the first of LGC1-4 that is BOE preferred.
segment_lgc AS (
    SELECT
        segmentid,
        lgc,
        b7sc,
        boe_preferred_lgc_flag,
        ROW_NUMBER() OVER (PARTITION BY segmentid ORDER BY preferred_lgc_flag <> 'Y', ogc_fid) AS lgc_rank
    FROM {{ source('recipe_sources', 'dcp_cscl_segment_lgc') }}
),
lgc AS (
    SELECT
        segmentid,
        MAX(CASE WHEN lgc_rank = 1 THEN lgc END) AS lgc1,
        MAX(CASE WHEN lgc_rank = 2 THEN lgc END) AS lgc2,
        MAX(CASE WHEN lgc_rank = 3 THEN lgc END) AS lgc3,
        MAX(CASE WHEN lgc_rank = 4 THEN lgc END) AS lgc4,
        MIN(CASE WHEN lgc_rank <= 4 AND boe_preferred_lgc_flag = 'Y' THEN lgc_rank END) AS boe_lgc_pointer,
        MAX(CASE WHEN lgc_rank = 1 THEN b7sc END) AS preferred_b7sc
    FROM segment_lgc
    GROUP BY segmentid
),
segment_streets AS (
    SELECT DISTINCT
        segmentid,
        b5sc
    FROM {{ ref('int__lgc') }}
)
SELECT
    -- An address point on a roadbed that reaches the generic file through the roadbed pointer
    -- list takes the generic segment's street name and face code (legacy SAFExtractClass)
    CASE
        WHEN address_points.segmentid <> saf.segmentid THEN generic_names.place_name_sort_order
        ELSE feature_names.place_name_sort_order
    END AS place_name,
    saf.boroughcode,
    CASE
        WHEN address_points.segmentid <> saf.segmentid
            THEN COALESCE(generic_names.face_code, SUBSTRING(saf.segment_lionkey, 2, 4))
        ELSE COALESCE(feature_names.face_code, SUBSTRING(saf.segment_lionkey, 2, 4))
    END AS face_code,
    SUBSTRING(saf.segment_lionkey, 6, 5) AS segment_seqnum,
    CASE
        WHEN address_points.sosindicator = '1' THEN 'L'
        WHEN address_points.sosindicator = '2' THEN 'R'
    END AS sos_indicator,
    LEFT(address_points.b7sc_actual, 6) AS b5sc,
    address_points.house_number AS hn,
    address_points.house_number_suffix AS hn_suffix,
    address_points.house_number_range_suffix AS high_alpha_hn_suffix,
    saf.saftype,
    -- An address point whose street isn't on its segment (e.g. JFK Airport addresses on Queens
    -- segments) takes LGC1 from its own B7SC, with LGC2-4 blank, as prod does
    CASE
        WHEN segment_streets.b5sc IS NOT NULL OR address_points.b7sc_actual IS NULL THEN lgc.lgc1
        ELSE RIGHT(address_points.b7sc_actual, 2)
    END AS lgc1,
    CASE WHEN segment_streets.b5sc IS NOT NULL OR address_points.b7sc_actual IS NULL THEN lgc.lgc2 END AS lgc2,
    CASE WHEN segment_streets.b5sc IS NOT NULL OR address_points.b7sc_actual IS NULL THEN lgc.lgc3 END AS lgc3,
    CASE WHEN segment_streets.b5sc IS NOT NULL OR address_points.b7sc_actual IS NULL THEN lgc.lgc4 END AS lgc4,
    CASE
        WHEN segment_streets.b5sc IS NOT NULL OR address_points.b7sc_actual IS NULL THEN lgc.boe_lgc_pointer
        ELSE 1
    END AS boe_lgc_pointer,
    saf.segment_type,
    saf.segmentid,
    saf.saf_globalid,
    saf.saf_source_table,
    address_points.addresspointid,
    feature_names.b7sc IS NOT NULL AS _joined_to_feature_name, -- for error report
    address_points.snd_feature_type,
    saf.generic,
    saf.roadbed
FROM saf
LEFT JOIN address_points ON saf.saf_globalid = address_points.globalid
LEFT JOIN feature_names -- TODO error when not joined
    ON address_points.b7sc_actual = feature_names.b7sc AND feature_names.feature_type = 'street'
LEFT JOIN lgc ON saf.segmentid = lgc.segmentid
LEFT JOIN feature_names AS generic_names
    ON lgc.preferred_b7sc = generic_names.b7sc AND generic_names.feature_type = 'street'
LEFT JOIN segment_streets
    ON saf.segmentid = segment_streets.segmentid AND LEFT(address_points.b7sc_actual, 6) = segment_streets.b5sc
WHERE saf.saftype = 'S'
