{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']}
    ]
) }}

WITH primary_segments AS (
    SELECT * FROM {{ ref("int__primary_segments") }}
),

primary_segment_fields AS (
    SELECT
        {{
            dbt_utils.star(from=ref("int__primary_segments"),
            except=["source_table", "boroughcode", "face_code", "segment_seqnum", "five_digit_street_code", "lgc1", "lgc2", "lgc3", "lgc4", "lgc5"])
        }}
    FROM {{ ref("int__primary_segments") }}
),

proto_segments AS (
    SELECT * FROM {{ ref("stg__altsegmentdata_proto") }}
),

segments AS (
    SELECT
        segmentid as segmentid_all,
        TRUE AS is_proto_segment,
        source_table,
        boroughcode,
        face_code,
        segment_seqnum,
        five_digit_street_code,
        lgc1,
        lgc2,
        lgc3,
        lgc4,
        lgc5
    FROM primary_segments
    UNION ALL
    SELECT
        segmentid as segmentid_all,
        FALSE AS is_proto_segment,
        source_table,
        boroughcode,
        face_code,
        segment_seqnum,
        five_digit_street_code,
        lgc1,
        lgc2,
        lgc3,
        lgc4,
        lgc5
    FROM proto_segments
),

all_segment_fields AS (
    SELECT
        segments.*,
        primary_segment_fields.*,
        primary_segment_fields.segmentid IS NOT NULL AS is_based_on_primary_segment
    FROM segments
    LEFT JOIN primary_segment_fields ON segments.segmentid_all = primary_segment_fields.segmentid
)
-- TODO flip left/right fields for relevant proto-segments

SELECT
    segmentid_all AS segmentid,
    segmentid AS primary_segmentid,
    is_proto_segment,
    is_based_on_primary_segment,
    boroughcode,
    face_code,
    segment_seqnum,
    five_digit_street_code,
    lgc1,
    lgc2,
    lgc3,
    lgc4,
    boe_lgc_pointer,
    from_sectionalmap,
    from_nodeid,
    from_x,
    from_y,
    to_sectionalmap,
    to_nodeid,
    to_x,
    to_y,
    left_2000_census_tract_basic,
    left_2000_census_tract_suffix,
    left_atomicid,
    l_low_hn,
    l_high_hn,
    lsubsect,
    l_zip,
    left_assembly_district,
    left_election_district,
    left_school_district,
    right_2000_census_tract_basic,
    right_2000_census_tract_suffix,
    right_atomicid,
    r_low_hn,
    r_high_hn,
    rsubsect,
    r_zip,
    right_assembly_district,
    right_election_district,
    right_school_district,
    split_election_district_flag,
    sandist_ind,
    traffic_direction,
    segment_locational_status,
    feature_type_code,
    nonped,
    continuous_parity_flag,
    borough_boundary_indicator,
    twisted_parity_flag,
    special_address_flag,
    curve_flag,
    center_of_curvature_x,
    center_of_curvature_y,
    segment_length_ft,
    from_level_code,
    to_level_code,
    trafdir_ver_flag,
    segment_type,
    coincident_seg_count,
    incex_flag,
    rw_type,
    physicalid,
    genericid,
    nypdid,
    fdnyid,
    status,
    streetwidth_min,
    streetwidth_irr,
    bike_lane,
    fcc,
    right_of_way_type,
    left_2010_census_tract_basic,
    left_2010_census_tract_suffix,
    right_2010_census_tract_basic,
    right_2010_census_tract_suffix,
    lgc5,
    lgc6,
    lgc7,
    lgc8,
    lgc9,
    legacy_segmentid,
    left_2000_census_block_basic,
    left_2000_census_block_suffix,
    right_2000_census_block_basic,
    right_2000_census_block_suffix,
    left_2010_census_block_basic,
    left_2010_census_block_suffix,
    right_2010_census_block_basic,
    right_2010_census_block_suffix,
    snow_priority,
    bike_lane_2,
    streetwidth_max,
    l_blockfaceid,
    r_blockfaceid,
    number_travel_lanes,
    number_park_lanes,
    number_total_lanes,
    bike_traffic_direction,
    posted_speed,
    left_nypd_service_area,
    right_nypd_service_area,
    truck_route_type,
    left_2020_census_tract_basic,
    left_2020_census_tract_suffix,
    right_2020_census_tract_basic,
    right_2020_census_tract_suffix,
    left_2020_census_block_basic,
    left_2020_census_block_suffix,
    right_2020_census_block_basic,
    right_2020_census_block_suffix,
    feature_type,
    source_table,
    geom,
    include_in_geosupport_lion,
    include_in_bytes_lion
FROM all_segment_fields
ORDER BY segmentid DESC, face_code DESC
