WITH saf AS (
    SELECT * FROM {{ ref('int__saf_segments') }}
),
address_points AS (
    SELECT * FROM {{ ref('int__address_points') }}
),
feature_names AS (
    SELECT * FROM {{ ref('stg__facecode_and_featurename_principal') }}
),
address_point_lgc AS (
    SELECT * FROM {{ ref('int__lgc_address_point') }}
)
SELECT
    -- TODO "warning" issued if featurename instead of streetname
    -- TODO error when null
    feature_names_vanity.place_name_sort_order AS place_name,
    saf.boroughcode,
    feature_names_actual.face_code,
    SUBSTRING(saf.segment_lionkey, 6, 5) AS segment_seqnum,
    CASE
        WHEN address_points.sosindicator = '1' THEN 'L'
        WHEN address_points.sosindicator = '2' THEN 'R'
    END AS sos_indicator,
    LEFT(address_points.b7sc_vanity, 6) AS b5sc,
    address_points.house_number AS low_hn,
    address_points.house_number_suffix AS low_hn_suffix,
    CASE
        WHEN address_points.hyphen_type IN ('N', 'Q', 'U') THEN address_points.house_number
        WHEN address_points.hyphen_type IN ('R', 'X') THEN address_points.house_number_range
    END AS high_hn,
    CASE
        WHEN address_points.hyphen_type IN ('N', 'Q', 'U') THEN address_points.house_number_suffix
        WHEN address_points.hyphen_type IN ('R', 'X') THEN address_points.house_number_range_suffix
    END AS high_hn_suffix,
    saf.saftype,
    RIGHT(address_points.b7sc_vanity, 2) AS lgc1,
    lgc.lgc2,
    lgc.lgc3,
    lgc.lgc4,
    COALESCE(lgc.boe_lgc_pointer, 1) AS boe_lgc_pointer,
    saf.segment_type,
    saf.segmentid,
    ROUND(ST_X(address_points.geom))::INT AS x_coord,
    ROUND(ST_Y(address_points.geom))::INT AS y_coord,
    saf.saf_globalid,
    saf.saf_source_table,
    address_points.addresspointid,
    saf.generic,
    saf.roadbed
FROM saf
LEFT JOIN address_points ON saf.saf_globalid = address_points.globalid
LEFT JOIN feature_names AS feature_names_vanity
    ON address_points.b7sc_vanity = feature_names_vanity.b7sc
LEFT JOIN feature_names AS feature_names_actual
    ON address_points.b7sc_actual = feature_names_actual.b7sc
LEFT JOIN address_point_lgc AS lgc ON address_points.addresspointid = lgc.addresspointid
WHERE saf.saftype = 'V'
