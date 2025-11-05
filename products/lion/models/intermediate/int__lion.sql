{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
      {'columns': ['boroughcode', 'segment_seqnum', 'segmentid']}
    ]
) }}

WITH segments AS (
    SELECT * FROM {{ ref("int__segments") }}
    WHERE is_based_on_primary_segment
),

segment_locational_status AS (
    SELECT * FROM {{ ref("int__segment_locational_status") }}
),

centerline AS (
    SELECT * FROM {{ ref("stg__centerline") }}
),

rail AS (
    SELECT * FROM {{ ref("stg__rail_and_subway" ) }}
),

nsf AS (
    SELECT * FROM {{ ref("stg__nonstreetfeatures") }}
),

atomic_polygons AS (
    SELECT * FROM {{ ref("int__segment_atomicpolygons") }}
),

nodes AS (
    SELECT * FROM {{ ref("int__segments_with_nodes") }}
),

saf AS (
    SELECT * FROM {{ ref("int__segment_specialaddress") }}
),

centerline_curve AS (
    SELECT * FROM {{ ref("int__centerline_curve") }}
),

sedat AS (
    SELECT * FROM {{ ref("int__split_election_district") }}
),

nypd_service_areas AS (
    SELECT * FROM {{ ref("int__segment_nypdbeat") }}
),

zips AS (
    SELECT * FROM {{ ref("int__segment_zipcodes") }}
),

centerline_coincident_subway_or_rail AS (
    SELECT * FROM {{ ref("int__centerline_coincident_subway_or_rail") }}
),

noncl_coincident_segment AS (
    SELECT * FROM {{ ref("int__noncenterline_coincident_segment_count") }}
)

SELECT
    segments.lionkey_dev,
    segments.boroughcode,
    segments.face_code,
    segments.segment_seqnum,
    segments.segmentid,
    segments.five_digit_street_code,
    segments.lgc1,
    segments.lgc2,
    segments.lgc3,
    segments.lgc4,
    segments.boe_lgc_pointer,
    nodes.from_sectionalmap,
    nodes.from_nodeid,
    nodes.from_x,
    nodes.from_y,
    nodes.to_sectionalmap,
    nodes.to_nodeid,
    nodes.to_x,
    nodes.to_y,
    ap_left.left_2000_census_tract_basic,
    ap_left.left_2000_census_tract_suffix,
    ap_left.left_atomicid,
    centerline.l_low_hn,
    centerline.l_high_hn,
    CASE
        WHEN (segment_locational_status.borough_boundary_indicator IS DISTINCT FROM 'L') THEN
            nullif(centerline.lsubsect, '0')
    END AS lsubsect,
    CASE
        WHEN (segment_locational_status.borough_boundary_indicator IS DISTINCT FROM 'L') THEN
            coalesce(centerline.l_zip, zips.l_zip)
    END AS l_zip,
    ap_left.left_assembly_district,
    ap_left.left_election_district,
    ap_left.left_school_district,
    ap_right.right_2000_census_tract_basic,
    ap_right.right_2000_census_tract_suffix,
    ap_right.right_atomicid,
    centerline.r_low_hn,
    centerline.r_high_hn,
    CASE
        WHEN (segment_locational_status.borough_boundary_indicator IS DISTINCT FROM 'R') THEN
            nullif(centerline.rsubsect, '0')
    END AS rsubsect,
    CASE
        WHEN (segment_locational_status.borough_boundary_indicator IS DISTINCT FROM 'R') THEN
            coalesce(centerline.r_zip, zips.r_zip)
    END AS r_zip,
    ap_right.right_assembly_district,
    ap_right.right_election_district,
    ap_right.right_school_district,
    sedat.split_election_district_flag,
    (ARRAY['L', 'R'])[centerline.sandist_ind::INT] AS sandist_ind,
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
    segment_locational_status.segment_locational_status,
    CASE
        WHEN segments.feature_type = 'centerline'
            THEN (
                CASE
                    -- Paper street that is not also a boundary 
                    WHEN centerline.status = '3' THEN '5'
                    -- Private street that exists physically 
                    WHEN centerline.status = '2' AND centerline.rwjurisdiction = '3' THEN '6'
                    -- Paper street that coincides with a non-physical boundary 
                    WHEN centerline.status = '9' THEN '9'
                    -- Alley
                    WHEN centerline.rw_type = 10 THEN 'A'
                    -- Path, non-vehicular, addressable
                    WHEN
                        centerline.trafdir = 'NV'
                        AND (
                            centerline.l_low_hn != '0'
                            OR centerline.l_high_hn != '0'
                            OR centerline.r_low_hn != '0'
                            OR centerline.r_high_hn != '0'
                        )
                        THEN 'W'
                    -- Ferry
                    WHEN centerline.rw_type = 14 THEN 'F'
                    -- Constructed
                    WHEN centerline.status = '2' AND centerline.rwjurisdiction = '5' THEN 'C'
                    -- Public street, bridge or tunnel that exists physically (or its generic geometry), other than Feature Type Code W  -- redundant but to be explicit about above case
                END
            )
        WHEN segments.feature_type = 'shoreline' THEN '2'
        WHEN segments.feature_type IN ('rail', 'subway') THEN '1'
        WHEN segments.feature_type = 'nonstreetfeatures' THEN (
            CASE
                -- Non-physical census block boundary
                WHEN nsf.linetype = 3 THEN '3'
                -- Non-physical boundary other than census
                WHEN nsf.linetype IN (1, 2, 6) THEN '7'
                -- Physical boundary such as cemetery wall
                WHEN nsf.linetype IN (4, 5) THEN '8'
                -- Other non-street feature
                WHEN nsf.linetype = 7 THEN '4'
            END
        )
    END AS feature_type_code,
    centerline.nonped,
    centerline.continuous_parity_flag,
    segment_locational_status.borough_boundary_indicator,
    CASE
        WHEN centerline.twisted_parity_flag = 'Y' THEN 'T'
    END AS twisted_parity_flag,
    saf.special_address_flag,
    CASE
        WHEN segments.feature_type = 'centerline' THEN centerline_curve.curve_flag
        WHEN st_numpoints(segments.geom) > 2 THEN 'I'
    END AS curve_flag,
    round(centerline_curve.center_of_curvature_x)::BIGINT AS center_of_curvature_x,
    round(centerline_curve.center_of_curvature_y)::BIGINT AS center_of_curvature_y,
    round(segments.shape_length)::INT AS segment_length_ft,
    convert_level_code(segments.from_level_code, segments.feature_type) AS from_level_code,
    convert_level_code(segments.to_level_code, segments.feature_type) AS to_level_code,
    centerline.trafdir_ver_flag,
    coalesce(centerline.segment_type, 'U') AS segment_type,
    CASE
        WHEN segments.feature_type = 'centerline'
            THEN
                centerline.coincident_seg_count - coalesce(centerline_coincident_subway_or_rail.subway_or_rail_count, 0)
        ELSE
            coalesce(noncl_coincident_segment.coincident_seg_count, 1)
    END AS coincident_seg_count,
    centerline.incex_flag,
    centerline.rw_type,
    centerline.physicalid,
    centerline.genericid,
    centerline.nypdid,
    centerline.fdnyid,
    centerline.status,
    centerline.streetwidth_min,
    centerline.streetwidth_irr,
    CASE
        WHEN centerline.bike_lane = '10' THEN 'A'
        WHEN centerline.bike_lane = '11' THEN 'B'
        ELSE centerline.bike_lane
    END AS bike_lane,
    centerline.fcc,
    rail.row_type AS right_of_way_type,
    ap_left.left_2010_census_tract_basic,
    ap_left.left_2010_census_tract_suffix,
    ap_right.right_2010_census_tract_basic,
    ap_right.right_2010_census_tract_suffix,
    segments.lgc5,
    segments.lgc6,
    segments.lgc7,
    segments.lgc8,
    segments.lgc9,
    segments.legacy_segmentid,
    ap_left.left_2000_census_block_basic,
    ap_left.left_2000_census_block_suffix,
    ap_right.right_2000_census_block_basic,
    ap_right.right_2000_census_block_suffix,
    ap_left.left_2010_census_block_basic,
    ap_left.left_2010_census_block_suffix,
    ap_right.right_2010_census_block_basic,
    ap_right.right_2010_census_block_suffix,
    centerline.snow_priority,
    centerline.bike_lane AS bike_lane_2,
    centerline.streetwidth_max,
    centerline.l_blockfaceid,
    centerline.r_blockfaceid,
    centerline.number_travel_lanes,
    centerline.number_park_lanes,
    centerline.number_total_lanes,
    centerline.bike_trafdir AS bike_traffic_direction,
    centerline.posted_speed,
    nypd_service_areas.left_nypd_service_area,
    nypd_service_areas.right_nypd_service_area,
    centerline.truck_route_type,
    ap_left.left_2020_census_tract_basic,
    ap_left.left_2020_census_tract_suffix,
    ap_right.right_2020_census_tract_basic,
    ap_right.right_2020_census_tract_suffix,
    ap_left.left_2020_census_block_basic,
    ap_left.left_2020_census_block_suffix,
    ap_right.right_2020_census_block_basic,
    ap_right.right_2020_census_block_suffix,
    segments.feature_type,
    segments.source_table,
    segments.geom,
    segments.include_in_geosupport_lion,
    segments.include_in_bytes_lion
FROM segments
LEFT JOIN nodes ON segments.lionkey_dev = nodes.lionkey_dev
LEFT JOIN segment_locational_status ON segments.lionkey_dev = segment_locational_status.lionkey_dev
LEFT JOIN
    atomic_polygons AS ap_left
    ON
        segments.lionkey_dev = ap_left.lionkey_dev
        AND segment_locational_status.borough_boundary_indicator IS DISTINCT FROM 'L'
LEFT JOIN
    atomic_polygons AS ap_right
    ON
        segments.lionkey_dev = ap_right.lionkey_dev
        AND segment_locational_status.borough_boundary_indicator IS DISTINCT FROM 'R'
LEFT JOIN saf ON segments.segmentid = saf.segmentid AND segments.boroughcode = saf.boroughcode
LEFT JOIN nypd_service_areas ON segments.lionkey_dev = nypd_service_areas.lionkey_dev
LEFT JOIN sedat ON segments.segmentid = sedat.segmentid AND segments.boroughcode = sedat.boroughcode
-- centerline only
LEFT JOIN centerline ON segments.segmentid = centerline.segmentid AND segments.feature_type = 'centerline'
LEFT JOIN centerline_curve ON centerline.segmentid = centerline_curve.segmentid AND segments.feature_type = 'centerline'
LEFT JOIN centerline_coincident_subway_or_rail ON centerline.segmentid = centerline_coincident_subway_or_rail.segmentid
-- shoreline only
-- rail only
LEFT JOIN rail ON segments.segmentid = rail.segmentid
-- nsf only
LEFT JOIN nsf ON segments.segmentid = nsf.segmentid
-- other
LEFT JOIN noncl_coincident_segment ON segments.segmentid = noncl_coincident_segment.segmentid
LEFT JOIN zips ON segments.lionkey_dev = zips.lionkey_dev AND segments.feature_type != 'centerline'
