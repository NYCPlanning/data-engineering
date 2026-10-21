WITH saf AS (
    SELECT * FROM {{ ref('int__saf_segments') }}
),
address_points AS (
    SELECT * FROM {{ ref('int__address_points') }}
),
feature_names AS (
    SELECT * FROM {{ ref('stg__facecode_and_featurename_principal') }}
),
lgc AS (
    SELECT * FROM {{ ref('int__streetcode_and_facecode') }}
),
segment_streets AS (
    SELECT DISTINCT
        segmentid,
        b5sc
    FROM {{ ref('int__lgc') }}
)
SELECT
    feature_names.place_name_sort_order AS place_name,
    saf.boroughcode,
    COALESCE(feature_names.face_code, SUBSTRING(saf.segment_lionkey, 2, 4)) AS face_code,
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
    -- segments) takes LGC1 from its own B7SC, with LGC2-4 blank (CSCL-SAF-01)
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
LEFT JOIN segment_streets
    ON saf.segmentid = segment_streets.segmentid AND LEFT(address_points.b7sc_actual, 6) = segment_streets.b5sc
WHERE saf.saftype = 'S'
