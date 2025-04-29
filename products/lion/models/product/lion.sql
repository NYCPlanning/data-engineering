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
)

SELECT
    centerline.boroughcode,
    NULL AS face_code,
    centerline.segment_seqnum,
    centerline.segmentid,
    NULL AS five_digit_street_code,
    NULL AS lgc1,
    NULL AS lgc2,
    NULL AS lgc3,
    NULL AS lgc4,
    NULL AS board_of_elections_lgc_pointer,
    nodes.from_sectionalmap,
    nodes.from_nodeid,
    nodes.from_x,
    nodes.from_y,
    nodes.to_sectionalmap,
    nodes.to_nodeid,
    nodes.to_x,
    nodes.to_y,
    ap.left_2000_census_tract, -- todo - in final formatting, suffix might need to be converted from 00s to blank if missing see 1.4
    ap.left_atomicid, -- TODO: "last 3 bytes"
    centerline.l_low_hn,
    centerline.l_high_hn,
    centerline.lsubsect, -- TODO: only 2 leftmost bytes
    centerline.l_zip,
    ap.left_assembly_district,
    ap.left_election_district,
    ap.left_school_district,
    ap.right_2000_census_tract,
    ap.right_atomicid, -- TODO: "last 3 bytes"
    centerline.r_low_hn,
    centerline.r_high_hn,
    centerline.rsubsect, -- TODO: only 2 leftmost bytes
    centerline.r_zip,
    ap.right_assembly_district,
    ap.right_election_district,
    ap.right_school_district,
    NULL AS split_election_district_flag,
    centerline.sandist_ind,
    NULL AS traffic_direction,
    NULL AS segment_locational_status,
    NULL AS feature_type_code,
    centerline.nonped,
    centerline.continuous_parity_flag,
    NULL AS borough_boundary_indicator,
    NULL AS twisted_parity_flag,
    saf.special_address_flag,
    NULL AS curve_flag,
    NULL AS center_of_curvature_x,
    NULL AS center_of_curvature_y,
    round(centerline.shape_length)::INT AS segment_length_ft,
    NULL AS from_level_code,
    NULL AS to_level_code,
    centerline.trafdir_ver_flag,
    centerline.segment_type,
    centerline.coincident_seg_count, -- TODO do not count subterranean subway/rail segments
    centerline.incex_flag,
    centerline.rw_type,
    centerline.physicalid,
    centerline.genericid,
    centerline.nypdid,
    centerline.fdnyid,
    centerline.status,
    centerline.streetwidth,
    centerline.streetwidth_irr,
    CASE
        WHEN centerline.bike_lane = '10' THEN 'A'
        WHEN centerline.bike_lane = '11' THEN 'B'
        ELSE centerline.bike_lane
    END AS bike_lane,
    centerline.fcc,
    NULL AS right_of_way_type, -- blank for centerline
    ap.left_2010_census_tract,
    ap.right_2010_census_tract,
    -- TODO: 2020 census tracts?
    NULL AS lgc5,
    NULL AS lgc6,
    NULL AS lgc7,
    NULL AS lgc8,
    NULL AS lgc9,
    centerline.legacy_segmentid,
    ap.left_2000_census_block_basic,
    ap.left_2000_census_block_suffix,
    ap.right_2000_census_block_basic,
    ap.right_2000_census_block_suffix,
    ap.left_2010_census_block_basic,
    ap.left_2010_census_block_suffix,
    ap.right_2010_census_block_basic,
    ap.right_2010_census_block_suffix,
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
    NULL AS left_nypd_service_area,
    NULL AS right_nypd_service_area,
    centerline.truck_route_type,
    ap.left_2020_census_tract,
    ap.right_2020_census_tract,
    ap.left_2020_census_block_basic,
    ap.left_2020_census_block_suffix,
    ap.right_2020_census_block_basic,
    ap.right_2020_census_block_suffix
FROM centerline
LEFT JOIN atomic_polygons AS ap ON centerline.segmentid = ap.segmentid
LEFT JOIN nodes ON centerline.segmentid = nodes.segmentid
LEFT JOIN saf ON centerline.segmentid = saf.segmentid AND centerline.boroughcode = saf.boroughcode
