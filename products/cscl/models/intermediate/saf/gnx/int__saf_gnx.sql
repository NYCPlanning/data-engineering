WITH saf AS (
    SELECT * FROM {{ ref('int__saf_segments') }}
),
commonplace AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_commonplace_gdb") }}
),
address_points AS (
    SELECT * FROM {{ ref('int__address_point_house_numbers') }}
),
feature_names AS (
    SELECT * FROM {{ ref('stg__facecode_and_featurename') }}
)
SELECT
    -- TODO "warning" issued if featurename instead of streetname
    -- TODO error when null
    -- TODO needs handling see 7.2 note 1.f
    feature_names.lookup_key AS place_name,
    saf.boroughcode,
    SUBSTRING(saf.segment_lionkey, 2, 4) AS face_code,
    SUBSTRING(saf.segment_lionkey, 6, 5) AS segment_seqnum,
    CASE
        WHEN commonplace.sosindicator = '1' THEN 'L'
        WHEN commonplace.sosindicator = '2' THEN 'R'
    END AS sos_indicator,
    LEFT(commonplace.b7sc, 6) AS b5sc,
    -- TODO there's a note about some text-processing when hyphen_type = 'R' and there's a hyphen in this field
    -- However, that does not match a single record. It seems like the docs are outdated and this is handled in cscl
    NULL AS l_low_hn,
    CASE WHEN commonplace.sosindicator = '1' THEN address_points.house_number END AS l_high_hn,
    NULL AS r_low_hn,
    CASE WHEN commonplace.sosindicator = '2' THEN address_points.house_number END AS r_high_hn,
    commonplace.saftype,
    RIGHT(commonplace.b7sc, 2) AS lgc1,
    NULL AS lgc2,
    NULL AS lgc3,
    NULL AS lgc4,
    '1' AS boe_lgc_pointer,
    saf.segment_type, -- todo weirdness about roadbed vs generic
    saf.segmentid, -- todo note 8
    ROUND(ST_X(commonplace.geom))::INT AS x_coord,
    ROUND(ST_Y(commonplace.geom))::INT AS y_coord,
    -- TODO have no idea what's up with these
    NULL AS side_borough_code,
    NULL AS side_ct2010,
    NULL AS side_ap,
    saf.generic,
    saf.roadbed
FROM saf
LEFT JOIN commonplace ON saf.saf_globalid = commonplace.globalid
LEFT JOIN address_points ON commonplace.primaryaddresspointid = address_points.addresspointid
LEFT JOIN feature_names ON commonplace.b7sc = feature_names.b7sc
WHERE saf.saftype IN ('G', 'N', 'X')
