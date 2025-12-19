WITH saf AS (
    SELECT * FROM {{ ref('int__saf_segments') }}
),
address_points AS (
    SELECT * FROM {{ ref('int__address_points') }}
),
feature_names AS (
    SELECT * FROM {{ ref('stg__facecode_and_featurename') }}
),
lgc AS (
    SELECT * FROM {{ ref('int__streetcode_and_facecode') }}
)
SELECT
    feature_names.saf_place_name AS place_name,
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
    lgc.lgc1,
    lgc.lgc2,
    lgc.lgc3,
    lgc.lgc4,
    lgc.boe_lgc_pointer,
    saf.segment_type,
    saf.segmentid,
    saf.generic,
    saf.roadbed,
    address_points.addresspointid,
    feature_names.b7sc IS NOT NULL AS _joined_to_feature_name, -- for error report
    address_points.snd_feature_type
FROM saf
LEFT JOIN address_points ON saf.saf_globalid = address_points.globalid
LEFT JOIN feature_names -- TODO error when not joined
    ON address_points.b7sc_actual = feature_names.b7sc AND feature_names.feature_type = 'street'
LEFT JOIN lgc ON saf.segmentid = lgc.segmentid
WHERE saf.saftype = 'S'
