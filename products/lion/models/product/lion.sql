WITH centerline AS (
    SELECT * FROM {{ ref("stg__centerline") }}
),

atomic_polygons AS (
    SELECT * FROM {{ ref("int__centerline_atomicpolygons") }}
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
    NULL AS from_sectionalmap,
    NULL AS from_nodeid,
    NULL AS from_x,
    NULL AS from_y,
    NULL AS to_sectionalmap,
    NULL AS to_nodeid,
    NULL AS to_x,
    NULL AS to_y,
    ap.left_2000_census_tract,
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
    NULL AS special_address_flag,
    NULL AS curve_flag,
    NULL AS center_of_curvature_x,
    NULL AS center_of_curvature_y,
    round(centerline.shape_length) AS segment_length_ft,
    NULL AS from_level_code,
    NULL AS to_level_code,
    centerline.trafdir_ver_flag,
    centerline.segment_type,
    centerline.coincident_seg_count,
    centerline.incex_flag,
    centerline.rw_type,
    centerline.physicalid,
    centerline.genericid,
    centerline.nypdid,
    centerline.fdnyid,
    centerline.l_blockfaceid,
    centerline.r_blockfaceid,
    centerline.status,
    centerline.streetwidth,
    centerline.streetwidth_irr,
    centerline.bike_lane,
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
    centerline.legacy_segmentid
FROM centerline
LEFT JOIN atomic_polygons AS ap ON centerline.segmentid = ap.segmentid
