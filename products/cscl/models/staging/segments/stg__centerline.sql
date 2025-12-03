{{ config(
    materialized = 'table',
    indexes=[ {'columns': ['segmentid']} ]
) }}

SELECT
    segmentid,
    fdnyid::INT,
    nypdid::INT,
    physicalid::INT,
    genericid::INT,
    legacy_segmentid,
    segment_type,
    segment_seqnum,
    coincident_seg_count,
    fcc,
    l_low_hn,
    l_high_hn,
    r_low_hn,
    r_high_hn,
    l_zip,
    r_zip,
    nullif(l_blockfaceid::INT, 0) AS l_blockfaceid, -- TODO - ingest read as int
    nullif(r_blockfaceid::INT, 0) AS r_blockfaceid, -- TODO - ingest read as int
    stname_label,
    avgtravtime,
    status,
    trafdir_ver_flag,
    rwjurisdiction,
    nominaldir,
    accessible,
    nonped,
    CASE
        WHEN bike_lane = '10' THEN 'A'
        WHEN bike_lane = '11' THEN 'B'
        ELSE bike_lane
    END AS bike_lane_1,
    bike_lane AS bike_lane_2,
    boroughcode,
    borough_indicator,
    seglocstatus,
    CASE
        WHEN sandist_ind = '1' THEN 'L'
        WHEN sandist_ind = '2' THEN 'R'
        ELSE ' '
    END AS sandist_ind,
    left(nullif(lsubsect, '0'), 2) AS lsubsect,
    left(nullif(rsubsect, '0'), 2) AS rsubsect,
    CASE
        WHEN continuous_parity_flag = '1' THEN 'L'
        WHEN continuous_parity_flag = '2' THEN 'R'
        ELSE ' '
    END AS continuous_parity_flag,
    twisted_parity_flag,
    curve,
    posted_speed::INT, -- TODO - ingest read as int
    incex_flag,
    segmentlength,
    streetwidth,
    streetwidth_irr,
    special_disaster,
    fire_lane,
    conversion_source,
    created_by,
    created_date,
    modified_by,
    modified_date,
    CASE
        -- With
        WHEN trafdir = 'FT' THEN 'W'
        -- Against
        WHEN trafdir = 'TF' THEN 'A'
        -- Non-vehicular
        WHEN trafdir = 'NV' THEN 'P'
        -- Two-way
        WHEN trafdir = 'TW' THEN 'T'
    END AS traffic_direction,
    rw_type,
    within_bndy_dist,
    truck_route_type,
    collectionmethod,
    from_level_code,
    to_level_code,
    globalid,
    snow_priority,
    carto_display_level,
    nullif(number_travel_lanes::INT, 0) AS number_travel_lanes, -- TODO - ingest read as int
    nullif(number_park_lanes::INT, 0) AS number_park_lanes, -- TODO - ingest read as int
    nullif(number_total_lanes::INT, 0) AS number_total_lanes, -- TODO - ingest read as int
    nullif(streetwidth_max::INT, 0) AS streetwidth_max, -- TODO - ingest read as int
    nullif(round(streetwidth_min)::INT, 0) AS streetwidth_min, -- TODO - ingest read as int
    bike_trafdir AS bike_traffic_direction,
    shape_length,
    geom,
    CASE
        -- Paper street that is not also a boundary 
        WHEN status = '3' THEN '5'
        -- Private street that exists physically 
        WHEN status = '2' AND rwjurisdiction = '3' THEN '6'
        -- Paper street that coincides with a non-physical boundary 
        WHEN status = '9' THEN '9'
        -- Alley
        WHEN rw_type = 10 THEN 'A'
        -- Path, non-vehicular, addressable
        WHEN
            trafdir = 'NV'
            AND (
                l_low_hn != '0'
                OR l_high_hn != '0'
                OR r_low_hn != '0'
                OR r_high_hn != '0'
            )
            THEN 'W'
        -- Ferry
        WHEN rw_type = 14 THEN 'F'
        -- Constructed
        WHEN status = '2' AND rwjurisdiction = '5' THEN 'C'
        -- Public street, bridge or tunnel that exists physically (or its generic geometry), other than Feature Type Code W  -- redundant but to be explicit about above case
    END AS feature_type_code,
    'centerline' AS feature_type,
    'centerline' AS source_table,
    (rwjurisdiction IS DISTINCT FROM '3' OR status = '2') AND rw_type != 8 AS include_in_geosupport_lion,
    (rwjurisdiction IS DISTINCT FROM '3' OR status = '2') AS include_in_bytes_lion
FROM {{ source('recipe_sources', 'dcp_cscl_centerline') }}
