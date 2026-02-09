WITH saf AS (
    SELECT * FROM {{ ref('int__saf_segments') }}
),
altsegdata AS (
    SELECT * FROM {{ ref('stg__altsegmentdata_saf') }}
),
feature_names AS (
    SELECT * FROM {{ ref('stg__facecode_and_featurename') }}
)
SELECT
    -- TODO "warning" issued if featurename instead of streetname
    -- TODO error when null
    CASE
        WHEN altsegdata.saftype = 'C' THEN '  75 STREET'
        ELSE feature_names.place_name_sort_order
    END AS place_name,
    saf.boroughcode,
    altsegdata.face_code,
    altsegdata.segment_seqnum,
    CASE
        WHEN altsegdata.sosindicator = '1' THEN 'L'
        WHEN altsegdata.sosindicator = '2' THEN 'R'
    END AS sos_indicator,
    altsegdata.b5sc,
    altsegdata.l_low_hn,
    altsegdata.l_high_hn,
    altsegdata.r_low_hn,
    altsegdata.r_high_hn,
    saf.saftype,
    altsegdata.lgc1,
    altsegdata.lgc2,
    altsegdata.lgc3,
    altsegdata.lgc4,
    altsegdata.boe_preferred_lgc_flag AS boe_lgc_pointer,
    saf.segment_type,
    altsegdata.segmentid,
    NULL::INT AS x_coord,
    NULL::INT AS y_coord,
    NULL AS side_borough_code,
    NULL::INT AS side_ct2020_basic,
    NULL::INT AS side_ct2020_suffix,
    NULL AS side_ap,
    saf.saf_globalid,
    saf.saf_source_table,
    saf.generic,
    saf.roadbed
FROM saf
INNER JOIN altsegdata ON saf.saf_globalid = altsegdata.globalid
LEFT JOIN feature_names
    ON (altsegdata.b5sc || altsegdata.lgc1) = feature_names.b7sc
WHERE saf.saftype IN ('A', 'B', 'C', 'E', 'P')
