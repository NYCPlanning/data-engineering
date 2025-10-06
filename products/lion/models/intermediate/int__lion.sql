{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
      {'columns': ['boroughcode', 'segment_seqnum', 'segmentid']}
    ]
) }}

WITH segments AS (
    SELECT * FROM {{ ref("int__segments") }}
),

centerline AS (
    SELECT * FROM {{ ref("int__centerline") }}
),

rail AS (
    SELECT * FROM {{ ref("int__rail_and_subway" ) }}
),

nsf AS (
    SELECT * FROM {{ ref("int__nonstreetfeatures") }}
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

street_and_facecode AS (
    SELECT * FROM {{ ref("int__streetcode_and_facecode") }}
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

segment_locational_status AS (
    SELECT * FROM {{ ref("int__segment_locational_status") }}
),

centerline_diff_coincident_segment AS (
    SELECT * FROM {{ ref("int__centerline_coincident_subway_or_rail") }}
),

noncl_coincident_segment AS (
    SELECT * FROM {{ ref("int__noncenterline_coincident_segment_count") }}
)

SELECT
    segments.boroughcode,
    street_and_facecode.face_code,
    segments.segment_seqnum,
    segments.segmentid,
    street_and_facecode.five_digit_street_code,
    street_and_facecode.lgc1,
    street_and_facecode.lgc2,
    street_and_facecode.lgc3,
    street_and_facecode.lgc4,
    street_and_facecode.boe_lgc_pointer::CHAR(1),
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
    centerline.lsubsect,
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
    centerline.rsubsect,
    CASE
        WHEN (segment_locational_status.borough_boundary_indicator IS DISTINCT FROM 'R') THEN
            coalesce(centerline.r_zip, zips.r_zip)
    END AS r_zip,
    ap_right.right_assembly_district,
    ap_right.right_election_district,
    ap_right.right_school_district,
    sedat.split_election_district_flag,
    centerline.sandist_ind,
    centerline.traffic_direction,
    segment_locational_status.segment_locational_status,
    segments.feature_type_code,
    centerline.nonped,
    centerline.continuous_parity_flag,
    segment_locational_status.borough_boundary_indicator,
    centerline.twisted_parity_flag,
    saf.special_address_flag,
    CASE
        WHEN segments.feature_type = 'centerline' THEN centerline_curve.curve_flag
        WHEN st_numpoints(segments.geom) > 2 THEN 'I'
    END AS curve_flag,
    round(centerline_curve.center_of_curvature_x)::BIGINT AS center_of_curvature_x,
    round(centerline_curve.center_of_curvature_y)::BIGINT AS center_of_curvature_y,
    round(segments.shape_length)::INT AS segment_length_ft,
    segments.from_level_code,
    segments.to_level_code,
    centerline.trafdir_ver_flag,
    segments.segment_type,
    CASE
        WHEN segments.source_feature_type = 'centerline'
            THEN
                centerline.cscl_coincident_seg_count
                - coalesce(centerline_diff_coincident_segment.subway_or_rail_count, 0)
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
    centerline.bike_lane,
    centerline.fcc,
    rail.right_of_way_type,
    ap_left.left_2010_census_tract_basic,
    ap_left.left_2010_census_tract_suffix,
    ap_right.right_2010_census_tract_basic,
    ap_right.right_2010_census_tract_suffix,
    street_and_facecode.lgc5,
    street_and_facecode.lgc6,
    street_and_facecode.lgc7,
    street_and_facecode.lgc8,
    street_and_facecode.lgc9,
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
    centerline.bike_lane_2,
    centerline.streetwidth_max,
    centerline.l_blockfaceid,
    centerline.r_blockfaceid,
    centerline.number_travel_lanes,
    centerline.number_park_lanes,
    centerline.number_total_lanes,
    centerline.bike_traffic_direction,
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
    segments.geom,
    segments.source_feature_type,
    segments.include_in_geosupport_lion,
    segments.include_in_bytes_lion
FROM segments
LEFT JOIN street_and_facecode ON segments.segmentid = street_and_facecode.segmentid
LEFT JOIN nodes ON segments.segmentid = nodes.segmentid
LEFT JOIN segment_locational_status ON segments.segmentid = segment_locational_status.segmentid
LEFT JOIN
    atomic_polygons AS ap_left
    ON
        segments.segmentid = ap_left.segmentid
        AND segment_locational_status.borough_boundary_indicator IS DISTINCT FROM 'L'
LEFT JOIN
    atomic_polygons AS ap_right
    ON
        segments.segmentid = ap_right.segmentid
        AND segment_locational_status.borough_boundary_indicator IS DISTINCT FROM 'R'
LEFT JOIN saf ON segments.segmentid = saf.segmentid AND segments.boroughcode = saf.boroughcode
LEFT JOIN nypd_service_areas ON segments.segmentid = nypd_service_areas.segmentid
LEFT JOIN sedat ON segments.segmentid = sedat.segmentid AND segments.boroughcode = sedat.boroughcode
-- centerline only
LEFT JOIN centerline ON segments.segmentid = centerline.segmentid
LEFT JOIN centerline_curve ON centerline.segmentid = centerline_curve.segmentid
LEFT JOIN centerline_diff_coincident_segment ON centerline.segmentid = centerline_diff_coincident_segment.segmentid
-- shoreline only
-- rail only
LEFT JOIN rail ON segments.segmentid = rail.segmentid
-- nsf only
LEFT JOIN nsf ON segments.segmentid = nsf.segmentid
-- other
LEFT JOIN noncl_coincident_segment ON segments.segmentid = noncl_coincident_segment.segmentid
LEFT JOIN zips ON segments.segmentid = zips.segmentid AND segments.source_feature_type <> 'centerline'
