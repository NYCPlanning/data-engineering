{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']}
    ]
) }}
SELECT
    boroughcode,
    segmentid,
    segment_seqnum,
    l_low_hn,
    l_high_hn,
    lsubsect,
    l_zip,
    r_low_hn,
    r_high_hn,
    rsubsect,
    r_zip,
    (ARRAY['L', 'R'])[sandist_ind::INT] AS sandist_ind,
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
                l_low_hn <> '0'
                OR l_high_hn <> '0'
                OR r_low_hn <> '0'
                OR r_high_hn <> '0'
            )
            THEN 'W'
        -- Ferry
        WHEN rw_type = 14 THEN 'F'
        -- Constructed
        WHEN status = '2' AND rwjurisdiction = '5' THEN 'C'
        -- Public street, bridge or tunnel that exists physically (or its generic geometry), other than Feature Type Code W  -- redundant but to be explicit about above case
    END AS feature_type_code,
    nonped,
    continuous_parity_flag,
    CASE
        WHEN twisted_parity_flag = 'Y' THEN 'T'
    END AS twisted_parity_flag,
    curve,
    convert_level_code(from_level_code) AS from_level_code,
    convert_level_code(to_level_code) AS to_level_code,
    trafdir_ver_flag,
    segment_type,
    coincident_seg_count AS cscl_coincident_seg_count,
    incex_flag,
    rw_type,
    physicalid,
    genericid,
    nypdid,
    fdnyid,
    status,
    streetwidth_min,
    streetwidth_irr,
    CASE
        WHEN bike_lane = '10' THEN 'A'
        WHEN bike_lane = '11' THEN 'B'
        ELSE bike_lane
    END AS bike_lane,
    fcc,
    legacy_segmentid,
    snow_priority,
    bike_lane AS bike_lane_2,
    streetwidth_max,
    l_blockfaceid,
    r_blockfaceid,
    number_travel_lanes,
    number_park_lanes,
    number_total_lanes,
    bike_trafdir AS bike_traffic_direction,
    posted_speed,
    truck_route_type,
    geom,
    shape_length,
    'centerline' AS source_feature_type,
    (rwjurisdiction IS DISTINCT FROM '3' OR status = '2') AND rw_type <> 8 AS include_in_geosupport_lion,
    (rwjurisdiction IS DISTINCT FROM '3' OR status = '2') AS include_in_bytes_lion
FROM {{ source("recipe_sources", "dcp_cscl_centerline") }}
