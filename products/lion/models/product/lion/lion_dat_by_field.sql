{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['"Segment ID"']},
      {'columns': ['"Borough"', '"Sequence Number"', '"Segment ID"']}
    ]
) }}

SELECT
    boroughcode AS "Borough",
    format_lion_text(face_code, 4, '0') AS "Face Code",
    format_lion_text(segment_seqnum, 5, '0') AS "Sequence Number",
    format_lion_text(segmentid::TEXT, 7, '0') AS "Segment ID",
    format_lion_text(five_digit_street_code, 5, '0') AS "5-Digit Street Code (5SC)",
    format_lion_text(lgc1, 2, '0') AS "LGC1",
    format_lion_text(lgc2, 2, '0', TRUE) AS "LGC2",
    format_lion_text(lgc3, 2, '0', TRUE) AS "LGC3",
    format_lion_text(lgc4, 2, '0', TRUE) AS "LGC4",
    coalesce(boe_lgc_pointer, ' ') AS "Board of Elections LGC Pointer",
    format_lion_text(from_sectionalmap, 2, '0') AS "From-Sectional Map",
    format_lion_text(from_nodeid::TEXT, 7, '0') AS "From-Node ID",
    format_lion_text(round(from_x)::INT::TEXT, 7, '0') AS "From-X Coordinate",
    format_lion_text(round(from_y)::INT::TEXT, 7, '0') AS "From-Y Coordinate",
    format_lion_text(to_sectionalmap, 2, '0') AS "To-Sectional Map",
    format_lion_text(to_nodeid::TEXT, 7, '0') AS "To-Node ID",
    format_lion_text(round(to_x)::INT::TEXT, 7, '0') AS "To-X Coordinate",
    format_lion_text(round(to_y)::INT::TEXT, 7, '0') AS "To-Y Coordinate",
    format_lion_text(left_2000_census_tract_basic::TEXT, 4, ' ') AS "Left 2000 Census Tract Basic",
    format_lion_text(left_2000_census_tract_suffix::TEXT, 2, '0', TRUE) AS "Left 2000 Census Tract Suffix",
    coalesce(right(left_atomicid, 3), '   ') AS "Left Dynamic Block",
    format_lion_text(l_low_hn, 7, ' ') AS "Left Low House Number",
    format_lion_text(l_high_hn, 7, ' ') AS "Left High House Number",
    format_lion_text(left(lsubsect, 2), 2, '0', TRUE) AS "Left Dept of Sanitation Subsection",
    format_lion_text(l_zip, 5, '0', TRUE) AS "Left Zip Code",
    format_lion_text(left_assembly_district, 2, '0', TRUE) AS "Left Assembly District",
    format_lion_text(left_election_district, 3, '0', TRUE) AS "Left Election District",
    format_lion_text(left_school_district, 2, '0', TRUE) AS "Left School District",
    format_lion_text(right_2000_census_tract_basic::TEXT, 4, ' ') AS "Right 2000 Census Tract Basic",
    format_lion_text(right_2000_census_tract_suffix::TEXT, 2, '0', TRUE) AS "Right 2000 Census Tract Suffix",
    coalesce(right(right_atomicid, 3), '   ') AS "Right Dynamic Block",
    format_lion_text(r_low_hn, 7, ' ') AS "Right Low House Number",
    format_lion_text(r_high_hn, 7, ' ') AS "Right High House Number",
    format_lion_text(left(rsubsect, 2), 2, '0', TRUE) AS "Right Dept of Sanitation Subsection",
    format_lion_text(r_zip, 5, '0', TRUE) AS "Right Zip Code",
    format_lion_text(right_assembly_district, 2, '0', TRUE) AS "Right Assembly District",
    format_lion_text(right_election_district, 3, '0', TRUE) AS "Right Election District",
    format_lion_text(right_school_district, 2, '0', TRUE) AS "Right School District",
    coalesce(split_election_district_flag, ' ') AS "Split Election District Flag",
    ' ' AS "Filler (formerly Split Community School District Flag)",
    coalesce(sandist_ind, ' ') AS "Sanitation District Boundary Indicator",
    coalesce(traffic_direction, ' ') AS "Traffic Direction",
    coalesce(segment_locational_status, ' ') AS "Segment Locational Status",
    coalesce(feature_type_code, ' ') AS "Feature Type Code",
    coalesce(nonped, ' ') AS "Non-Pedestrian Flag",
    CASE
        WHEN continuous_parity_flag = '1' THEN 'L'
        WHEN continuous_parity_flag = '2' THEN 'R'
        ELSE ' '
    END AS "Continuous Parity Indicator",
    ' ' AS "Filler (formerly the Near BQ-Boundary Flag)",
    coalesce(borough_boundary_indicator, ' ') AS "Borough Boundary Indicator",
    coalesce(twisted_parity_flag, ' ') AS "Twisted Parity Flag",
    coalesce(special_address_flag, ' ') AS "Special Address Flag",
    coalesce(curve_flag, ' ') AS "Curve Flag",
    format_lion_text(center_of_curvature_x::TEXT, 7, '0') AS "Center of Curvature X-Coordinate",
    format_lion_text(center_of_curvature_y::TEXT, 7, '0') AS "Center of Curvature Y-Coordinate",
    format_lion_text(segment_length_ft::TEXT, 5, '0') AS "Segment Length in Feet",
    coalesce(from_level_code, ' ') AS "From Level Code",
    coalesce(to_level_code, ' ') AS "To Level Code",
    coalesce(trafdir_ver_flag, ' ') AS "Traffic Direction Verification Flag",
    segment_type AS "Segment Type Code",
    coalesce(coincident_seg_count::TEXT, ' ') AS "Coincident Segment Counter",
    coalesce(incex_flag, ' ') AS "Include/Exclude Flag",
    format_lion_text(rw_type::TEXT, 2, ' ') AS "Roadway Type",
    format_lion_text(physicalid::INT::TEXT, 7, '0', TRUE) AS "PHYSICALID", -- TODO - ingest read as int
    format_lion_text(genericid::INT::TEXT, 7, '0', TRUE) AS "GENERICID", -- TODO - ingest read as int
    format_lion_text(nypdid::INT::TEXT, 7, '0', TRUE) AS "NYPDID", -- TODO - ingest read as int
    format_lion_text(fdnyid::INT::TEXT, 7, '0', TRUE) AS "FDNYID", -- TODO - ingest read as int
    '       ' AS "Filler (formerly Left BLOCKFACEID)",
    '       ' AS "Filler (formerly Right BLOCKFACEID)",
    coalesce(status, ' ') AS "STATUS",
    format_lion_text(nullif(round(streetwidth_min)::TEXT, '0'), 3, ' ') AS "STREETWIDTH_MIN",
    coalesce(streetwidth_irr, ' ') AS "STREETWIDTH_IRR",
    coalesce(bike_lane, ' ') AS "BIKELANE_1",
    coalesce(fcc, '  ') AS "FCC",
    coalesce(right_of_way_type, ' ') AS "Right of Way Type",
    format_lion_text(left_2010_census_tract_basic::TEXT, 4, ' ') AS "Left 2010 Census Tract Basic",
    format_lion_text(left_2010_census_tract_suffix::TEXT, 2, '0', TRUE) AS "Left 2010 Census Tract Suffix",
    format_lion_text(right_2010_census_tract_basic::TEXT, 4, ' ') AS "Right 2010 Census Tract Basic",
    format_lion_text(right_2010_census_tract_suffix::TEXT, 2, '0', TRUE) AS "Right 2010 Census Tract Suffix",
    format_lion_text(lgc5, 2, '0', TRUE) AS "LGC5",
    format_lion_text(lgc6, 2, '0', TRUE) AS "LGC6",
    format_lion_text(lgc7, 2, '0', TRUE) AS "LGC7",
    format_lion_text(lgc8, 2, '0', TRUE) AS "LGC8",
    format_lion_text(lgc9, 2, '0', TRUE) AS "LGC9",
    format_lion_text(legacy_segmentid::INT::TEXT, 7, '0', TRUE) AS "Legacy SEGMENTID", -- TODO - ingest read as int
    format_lion_text(left_2000_census_block_basic::TEXT, 4, ' ') AS "LEFT CENSUS BLOCK 2000 BASIC",
    format_lion_text(left_2000_census_block_suffix, 1, ' ') AS "LEFT CENSUS BLOCK 2000 SUFFIX",
    format_lion_text(right_2000_census_block_basic::TEXT, 4, ' ') AS "RIGHT CENSUS BLOCK 2000 BASIC",
    format_lion_text(right_2000_census_block_suffix::TEXT, 1, ' ') AS "RIGHT CENSUS BLOCK 2000 SUFFIX",
    format_lion_text(left_2010_census_block_basic::TEXT, 4, ' ') AS "LEFT CENSUS BLOCK 2010 BASIC",
    format_lion_text(left_2010_census_block_suffix::TEXT, 1, ' ') AS "LEFT CENSUS BLOCK 2010 SUFFIX",
    format_lion_text(right_2010_census_block_basic::TEXT, 4, ' ') AS "RIGHT CENSUS BLOCK 2010 BASIC",
    format_lion_text(right_2010_census_block_suffix::TEXT, 1, ' ') AS "RIGHT CENSUS BLOCK 2010 SUFFIX",
    coalesce(snow_priority, ' ') AS "SNOW PRIORITY",
    format_lion_text(bike_lane_2, 2, ' ') AS "BIKELANE_2",
    format_lion_text(nullif(streetwidth_max::INT::TEXT, '0'), 3, ' ') AS "STREET WIDTH MAX", -- TODO - ingest read as int
    '   ' AS "Filler L89",
    format_lion_text(nullif(l_blockfaceid::INT::TEXT, '0'), 10, '0', TRUE) AS "Left BLOCKFACEID", -- TODO - ingest read as int
    format_lion_text(nullif(r_blockfaceid::INT::TEXT, '0'), 10, '0', TRUE) AS "Right BLOCKFACEID", -- TODO - ingest read as int
    format_lion_text(nullif(number_travel_lanes::INT::TEXT, '0'), 2, ' ') AS "NUMBER TRAVEL LANES", -- TODO - ingest read as int
    format_lion_text(nullif(number_park_lanes::INT::TEXT, '0'), 2, ' ') AS "NUMBER PARK LANES", -- TODO - ingest read as int
    format_lion_text(nullif(number_total_lanes::INT::TEXT, '0'), 2, ' ') AS "NUMBER TOTAL LANES", -- TODO - ingest read as int
    format_lion_text(bike_traffic_direction, 2, ' ') AS "BIKE TRAFFIC DIR",
    format_lion_text(posted_speed::INT::TEXT, 2, ' ') AS "POSTED SPEED", -- TODO - ingest read as int
    format_lion_text(left_nypd_service_area, 1, ' ') AS "Left NYPD Service Area",
    format_lion_text(right_nypd_service_area, 1, ' ') AS "Right NYPD Service Area",
    format_lion_text(truck_route_type, 1, ' ') AS "Truck Route Type",
    format_lion_text(left_2020_census_tract_basic::TEXT, 4, ' ') AS "LEFT 2020 CENSUS TRACT Basic",
    format_lion_text(left_2020_census_tract_suffix::TEXT, 2, '0', TRUE) AS "LEFT 2020 CENSUS TRACT Suffix",
    format_lion_text(right_2020_census_tract_basic::TEXT, 4, ' ') AS "RIGHT 2020 CENSUS TRACT Basic",
    format_lion_text(right_2020_census_tract_suffix::TEXT, 2, '0', TRUE) AS "RIGHT 2020 CENSUS TRACT Suffix",
    format_lion_text(left_2020_census_block_basic::TEXT, 4, ' ') AS "LEFT CENSUS BLOCK 2020 BASIC",
    coalesce(left_2020_census_block_suffix::TEXT, ' ') AS "LEFT CENSUS BLOCK 2020 SUFFIX",
    format_lion_text(right_2020_census_block_basic::TEXT, 4, ' ') AS "RIGHT CENSUS BLOCK 2020 BASIC",
    coalesce(right_2020_census_block_suffix::TEXT, ' ') AS "RIGHT CENSUS BLOCK 2020 SUFFIX",
    format_lion_text('', 45, ' ') AS "Filler L199"
FROM {{ ref("int__lion") }}
