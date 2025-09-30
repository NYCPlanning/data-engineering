{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
      {'columns': ['boroughcode', 'segment_seqnum', 'segmentid']}
    ]
) }}

WITH centerline AS (
    SELECT * FROM {{ ref("stg__centerline") }}
),

atomic_polygons AS (
    SELECT * FROM {{ ref("int__centerline_atomicpolygons") }}
),

nodes AS (
    SELECT * FROM {{ ref("int__centerline_segments_with_nodes") }}
),

saf AS (
    SELECT * FROM {{ ref("int__centerline_specialaddress") }}
),

curve AS (
    SELECT * FROM {{ ref("int__centerline_curve") }}
),

streets AS (
    SELECT * FROM {{ ref("int__centerline_streetcode_and_facecode") }}
),

sedat AS (
    SELECT * FROM {{ ref("int__split_election_district") }}
),

nypd_service_areas AS (
    SELECT * FROM {{ ref("int__centerline_nypdbeat") }}
),

segment_locational_status AS (
    SELECT * FROM {{ ref("int__centerline_segment_locational_status") }}
),

diff_coincident_segment AS (
    SELECT * FROM {{ ref("int__centerline_coincident_subway_or_rail") }}
)

SELECT
    centerline.boroughcode,
    streets.face_code,
    centerline.segment_seqnum,
    centerline.segmentid,
    streets.five_digit_street_code,
    streets.lgc1,
    streets.lgc2,
    streets.lgc3,
    streets.lgc4,
    streets.boe_lgc_pointer::CHAR(1),
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
    centerline.l_zip,
    ap_left.left_assembly_district,
    ap_left.left_election_district,
    ap_left.left_school_district,
    ap_right.right_2000_census_tract_basic,
    ap_right.right_2000_census_tract_suffix,
    ap_right.right_atomicid,
    centerline.r_low_hn,
    centerline.r_high_hn,
    centerline.rsubsect,
    centerline.r_zip,
    ap_right.right_assembly_district,
    ap_right.right_election_district,
    ap_right.right_school_district,
    sedat.split_election_district_flag,
    (ARRAY['L', 'R'])[centerline.sandist_ind::INT] AS sandist_ind,
    CASE
        WHEN trafdir = 'FT' THEN 'W'
        WHEN trafdir = 'TF' THEN 'A'
        WHEN trafdir = 'NV' THEN 'P'
        WHEN trafdir = 'TW' THEN 'T'
    END AS traffic_direction,
    segment_locational_status.segment_locational_status,
    CASE
        WHEN status = '3' THEN '5'
        WHEN status = '2' AND rwjurisdiction = '3' THEN '6'
        WHEN status = '9' THEN '9'
        WHEN rw_type = 10 THEN 'A'
        WHEN trafdir = 'NV' AND (l_low_hn <> '0' OR l_high_hn <> '0' OR r_low_hn <> '0' OR r_high_hn <> '0') THEN 'W'
        WHEN rw_type = 14 THEN 'F'
        WHEN status = '2' AND rwjurisdiction = '5' THEN 'C'
    END AS feature_type_code,
    centerline.nonped,
    centerline.continuous_parity_flag,
    segment_locational_status.borough_boundary_indicator,
    CASE
        WHEN twisted_parity_flag = 'Y' THEN 'T'
    END AS twisted_parity_flag,
    saf.special_address_flag,
    curve.curve_flag,
    round(curve.center_of_curvature_x)::BIGINT AS center_of_curvature_x,
    round(curve.center_of_curvature_y)::BIGINT AS center_of_curvature_y,
    round(centerline.shape_length)::INT AS segment_length_ft,
    CASE
        WHEN from_level_code BETWEEN 1 AND 26 THEN chr(64 + from_level_code)
        WHEN from_level_code = 99 THEN '*'
    END AS from_level_code,
    CASE
        WHEN to_level_code BETWEEN 1 AND 26 THEN chr(64 + to_level_code)
        WHEN to_level_code = 99 THEN '*'
    END AS to_level_code,
    centerline.trafdir_ver_flag,
    centerline.segment_type,
    centerline.coincident_seg_count - coalesce(diff_coincident_segment.subway_or_rail_count, 0) AS coincident_seg_count,
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
    NULL AS right_of_way_type, -- blank for centerline
    ap_left.left_2010_census_tract_basic,
    ap_left.left_2010_census_tract_suffix,
    ap_right.right_2010_census_tract_basic,
    ap_right.right_2010_census_tract_suffix,
    streets.lgc5,
    streets.lgc6,
    streets.lgc7,
    streets.lgc8,
    streets.lgc9,
    centerline.legacy_segmentid,
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
    ap_right.right_2020_census_block_suffix
FROM centerline
LEFT JOIN nodes ON centerline.segmentid = nodes.segmentid
LEFT JOIN saf ON centerline.segmentid = saf.segmentid AND centerline.boroughcode = saf.boroughcode
LEFT JOIN curve ON centerline.segmentid = curve.segmentid
LEFT JOIN streets ON centerline.segmentid = streets.segmentid
LEFT JOIN sedat ON centerline.segmentid = sedat.segmentid AND centerline.boroughcode = sedat.boroughcode
LEFT JOIN nypd_service_areas ON centerline.segmentid = nypd_service_areas.segmentid
LEFT JOIN segment_locational_status ON centerline.segmentid = segment_locational_status.segmentid
LEFT JOIN
    atomic_polygons AS ap_left
    ON
        centerline.segmentid = ap_left.segmentid
        AND segment_locational_status.borough_boundary_indicator IS DISTINCT FROM 'L'
LEFT JOIN
    atomic_polygons AS ap_right
    ON
        centerline.segmentid = ap_right.segmentid
        AND segment_locational_status.borough_boundary_indicator IS DISTINCT FROM 'R'
LEFT JOIN diff_coincident_segment ON centerline.segmentid = diff_coincident_segment.segmentid
