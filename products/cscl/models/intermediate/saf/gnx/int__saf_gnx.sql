WITH commonplace AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_commonplace_gdb") }}
),
centerline AS (
    SELECT * FROM {{ ref('stg__centerline') }}
),
proto AS (
    SELECT * FROM {{ ref('stg__altsegmentdata_proto') }}
),
shoreline AS (
    SELECT * FROM {{ ref('stg__shoreline') }}
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
    feature_name.lookup_key AS place_name
    commonplace.boroughcode,
    '' AS face_code,
    '' AS segment_seqnum,
    CASE
        WHEN commonplace.sosindicator = '1' THEN 'L'
        WHEN commonplace.sosindicator = '2' THEN 'R'
    END AS sos_indicator,
    commonplace.b5sc,
    -- TODO there's a note about some text-processing when hyphen_type = 'R' and there's a hyphen in this field
    -- However, that does not match a single record. It seems like the docs are outdated and this is handled in cscl
    CASE WHEN commonplace.sosindicator = '1' THEN address_points.house_number END AS l_high_hn,
    CASE WHEN commonplace.sosindicator = '2' THEN address_points.house_number END AS r_high_hn,
    commonplace.saftype,
    RIGHT(commonplace.b7sc, 2) AS nap_lgc1,
    1 AS boe_lgc_pointer,
    CASE
        WHEN shoreline.segmentid IS NOT NULL THEN 'U'
        ELSE centerline.segment_type -- todo weirdness about roadbed vs generic
    END AS segment_type,
    saf.segmentid, -- todo note 8
    ROUND(ST_X(commonplace.geom))::INT AS x_coord,
    ROUND(ST_Y(commonplace.geom))::INT AS y_coord,
    -- TODO have no idea what's up with these
    NULL AS side_borough_code,
    NULL AS side_ct2010,
    NULL AS side_ap
FROM commonplace
LEFT JOIN segments ON saf.segmentid = segments.segmentid -- this is actually quite complicated
LEFT JOIN address_points ON commonplace.primaryaddresspointid = address_points.addresspointid
LEFT JOIN feature_names
    ON (saf.b5sc || saf.lgc1) = feature_names.b7sc
WHERE saf.saftype IN ('A', 'B', 'C', 'E', 'P')
