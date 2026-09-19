{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Base LION rows (one per segment/proto-segment) plus SAF-replicant rows (one per SAF
-- datum associated with a segment - ETL spec §2.7.2 "Segment Replication for SAF Data").
-- A replicant has identical geometry/attributes to its base row except for SpecAddr,
-- SAFStreetName/SAFStreetCode (fields that only exist on replicants), house
-- numbers/zip, and Join_ID - see int__saf_replicant_fields.sql for the override values
-- and its documented v1 simplifications (continuous-parity duplication and S/V zip
-- aren't implemented).

WITH lion AS (
    SELECT * FROM {{ ref('int__lion') }}
    WHERE include_in_bytes_lion
),
saf_replicants AS (
    SELECT * FROM {{ ref('int__saf_replicant_fields') }}
),

base_rows AS (
    SELECT
        street AS "Street",
        NULL::text AS "SAFStreetName",
        -- Prod's literal value for the default (no-specific-type) case is '0', not
        -- blank - confirmed against prod's FeatureTyp (190,050 rows citywide). Left as
        -- NULL upstream (see stg__centerline.sql) since int__protosegments.sql keys a
        -- different join off that NULL; only substituted here, at the output boundary.
        coalesce(feature_type_code, '0') AS "FeatureTyp",
        segment_type AS "SegmentTyp",
        incex_flag AS "IncExFlag",
        -- ETL spec §2.7.3 (BL6): derived from IncExFlag (Geosupport L58) and SegmentTyp (L56).
        CASE
            WHEN incex_flag = 'E' THEN 'N'
            WHEN segment_type IN ('C', 'R', 'S', 'T') THEN 'R'
            WHEN segment_type IN ('G', 'F') THEN 'G'
            ELSE 'B'
        END AS "RB_Layer",
        nonped AS "NonPed",
        traffic_direction AS "TrafDir",
        -- ETL spec §2.7.3 (BL8): TRAFDIR_VER_FLAG 'F'/'P'/'T' -> 'FLD'/'DCP'/'DOT'.
        CASE trafdir_ver_flag
            WHEN 'F' THEN 'FLD'
            WHEN 'P' THEN 'DCP'
            WHEN 'T' THEN 'DOT'
        END AS "TrafSrc",
        -- ETL spec §2.7.3: "SpecAddr will be populated with a blank in all output
        -- records other than SAF replicants" - now that replicant_rows (below) carries
        -- the real SAF type, the base row's own approximation (special_address_flag,
        -- used pre-replication) would double-count the same SAF association.
        NULL::text AS "SpecAddr",
        face_code AS "FaceCode",
        segment_seqnum AS "SeqNum",
        five_digit_street_code AS "StreetCode",
        NULL::text AS "SAFStreetCode",
        lgc1 AS "LGC1",
        lgc2 AS "LGC2",
        lgc3 AS "LGC3",
        lgc4 AS "LGC4",
        lgc5 AS "LGC5",
        lgc6 AS "LGC6",
        lgc7 AS "LGC7",
        lgc8 AS "LGC8",
        lgc9 AS "LGC9",
        boe_lgc_pointer AS "BOE_LGC",
        lpad(segmentid::text, 7, '0') AS "SegmentID",
        coincident_seg_count::text AS "SegCount",
        segment_locational_status AS "LocStatus",
        l_zip AS "LZip",
        r_zip AS "RZip",
        -- ETL spec §2.7.3 (BL29/30): the segment "belongs" to boroughcode. When BoroBndry
        -- marks one side as out-of-borough, that side's Boro field is null and the other
        -- side is populated with boroughcode instead of its own (out-of-borough) side.
        CASE WHEN borough_boundary_indicator = 'L' THEN NULL ELSE boroughcode::int END AS "LBoro",
        CASE WHEN borough_boundary_indicator = 'R' THEN NULL ELSE boroughcode::int END AS "RBoro",
        left_community_district AS "L_CD",
        right_community_district AS "R_CD",
        left_dynamic_block AS "LATOMICPOLYGON",
        right_dynamic_block AS "RATOMICPOLYGON",
        left_2020_census_tract_basic::text AS "LCT2020",
        left_2020_census_tract_suffix::text AS "LCT2020Suf",
        right_2020_census_tract_basic::text AS "RCT2020",
        right_2020_census_tract_suffix::text AS "RCT2020Suf",
        left_2020_census_block_basic::text AS "LCB2020",
        left_2020_census_block_suffix AS "LCB2020Suf",
        right_2020_census_block_basic::text AS "RCB2020",
        right_2020_census_block_suffix AS "RCB2020Suf",
        left_2010_census_tract_basic::text AS "LCT2010",
        left_2010_census_tract_suffix::text AS "LCT2010Suf",
        right_2010_census_tract_basic::text AS "RCT2010",
        right_2010_census_tract_suffix::text AS "RCT2010Suf",
        left_2010_census_block_basic::text AS "LCB2010",
        left_2010_census_block_suffix AS "LCB2010Suf",
        right_2010_census_block_basic::text AS "RCB2010",
        right_2010_census_block_suffix AS "RCB2010Suf",
        left_2000_census_tract_basic::text AS "LCT2000",
        left_2000_census_tract_suffix::text AS "LCT2000Suf",
        right_2000_census_tract_basic::text AS "RCT2000",
        right_2000_census_tract_suffix::text AS "RCT2000Suf",
        left_2000_census_block_basic::text AS "LCB2000",
        left_2000_census_block_suffix::text AS "LCB2000Suf",
        right_2000_census_block_basic::text AS "RCB2000",
        right_2000_census_block_suffix::text AS "RCB2000Suf",
        left_1990_census_tract_basic::text AS "LCT1990",
        left_1990_census_tract_suffix::text AS "LCT1990Suf",
        right_1990_census_tract_basic::text AS "RCT1990",
        right_1990_census_tract_suffix::text AS "RCT1990Suf",
        left_assembly_district AS "LAssmDist",
        left_election_district AS "LElectDist",
        right_assembly_district AS "RAssmDist",
        right_election_district AS "RElectDist",
        split_election_district_flag AS "SplitElect",
        left_school_district AS "LSchlDist",
        right_school_district AS "RSchlDist",
        -- ETL spec §2.7.3 (BL62): "blank, it is one digit filler in Geosupport LION" - not
        -- a derived field. Confirmed 100% blank in prod (production_outputs.fgdb_lion).
        NULL::text AS "SplitSchl",
        lsubsect AS "LSubSect",
        rsubsect AS "RSubSect",
        sandist_ind AS "SanDistInd",
        from_sectionalmap AS "MapFrom",
        to_sectionalmap AS "MapTo",
        borough_boundary_indicator AS "BoroBndry",
        -- ETL spec §2.7.3: 'M' if the segment belongs to Manhattan and either side is
        -- Manhattan/2000-tract-309; 'R' if it belongs to the Bronx and either side is
        -- Bronx/2000-tract-1; else null.
        CASE
            WHEN
                boroughcode::int = 1
                AND (
                    (left_borocode::int = 1 AND left_2000_census_tract_basic = 309)
                    OR (right_borocode::int = 1 AND right_2000_census_tract_basic = 309)
                )
                THEN 'M'
            WHEN
                boroughcode::int = 2
                AND (
                    (left_borocode::int = 2 AND left_2000_census_tract_basic = 1)
                    OR (right_borocode::int = 2 AND right_2000_census_tract_basic = 1)
                )
                THEN 'R'
        END AS "MH_RI_Flag",
        from_x AS "XFrom",
        from_y AS "YFrom",
        to_x AS "XTo",
        to_y AS "YTo",
        center_of_curvature_x::int AS "ArcCenterX",
        center_of_curvature_y::int AS "ArcCenterY",
        curve_flag AS "CurveFlag",
        -- Tied to the ArcCenterX/Y curve-geometry issue (CSCL-LION-07, data_issues.md) -
        -- on hold, not implemented here.
        NULL::int AS "Radius",
        lpad(from_nodeid::text, 7, '0') AS "NodeIDFrom",
        lpad(to_nodeid::text, 7, '0') AS "NodeIDTo",
        from_level_code AS "NodeLevelF",
        to_level_code AS "NodeLevelT",
        continuous_parity_flag AS "ConParity",
        twisted_parity_flag AS "Twisted",
        rw_type::text AS "RW_TYPE",
        physicalid::int AS "PhysicalID",
        genericid::int AS "GenericID",
        nypdid::text AS "NYPDID",
        fdnyid AS "FDNYID",
        l_blockfaceid::text AS "LBlockFaceID",
        r_blockfaceid::text AS "RBlockFaceID",
        -- Prod's convention for "no legacy ID" is a literal '0000000', not blank/null -
        -- coalesce before padding so segments without one match that instead of NULL.
        lpad(coalesce(legacy_segmentid, 0)::text, 7, '0') AS "LegacyID",
        status AS "Status",
        streetwidth_min::float AS "StreetWidth_Min",
        streetwidth_max::float AS "StreetWidth_Max",
        streetwidth_irr AS "StreetWidth_Irr",
        coalesce(bike_lane_1, ' ') || coalesce(bike_lane_2, ' ') AS "BikeLane",
        bike_traffic_direction AS "BIKE_TRAFDIR",
        -- ETL spec §2.7.3 (BL113): "Subway Active Flag" - only populated for Subway-sourced
        -- segments; null for Centerline/Rail.
        active_flag AS "ACTIVE_FLAG",
        posted_speed::text AS "POSTED_SPEED",
        snow_priority AS "Snow_Priority",
        number_travel_lanes::text AS "Number_Travel_Lanes",
        number_park_lanes::text AS "Number_Park_Lanes",
        number_total_lanes::text AS "Number_Total_Lanes",
        carto_display_level AS "Carto_Display_Level",
        fcc AS "FCC",
        right_of_way_type AS "ROW_Type",
        nullif(l_low_hn, '0') AS "LLo_Hyphen",
        nullif(l_high_hn, '0') AS "LHi_Hyphen",
        nullif(r_low_hn, '0') AS "RLo_Hyphen",
        nullif(r_high_hn, '0') AS "RHi_Hyphen",
        -- ETL spec §2.7.3 (BL101-104): normalized Queens-hyphen house numbers, zeroed out
        -- for certain segment types (Generic, non-physical, etc). Investigated 2026-09-18:
        -- real prod data contradicts a literal reading of the zero-out rule (SegmentTyp='G'
        -- rows exist in prod with nonzero FromRight/ToRight), so this needs more targeted
        -- investigation before implementing - left unimplemented for now.
        NULL::int AS "FromLeft",
        NULL::int AS "ToLeft",
        NULL::int AS "FromRight",
        NULL::int AS "ToRight",
        -- Join_ID links each segment to its AltNames entries (ETL spec §2.7.3). Non-SAF
        -- form; SAF replicants (below) use a different encoding.
        {{ lion_join_id() }} AS "Join_ID",
        left_nypd_service_area AS "L_PD_Service_Area",
        right_nypd_service_area AS "R_PD_Service_Area",
        truck_route_type AS "TRUCK_ROUTE_TYPE",
        st_length(geom)::float AS "SHAPE_Length",
        geom
    FROM lion
),

replicant_rows AS (
    SELECT
        lion.street AS "Street",
        saf_replicants.saf_street_name AS "SAFStreetName",
        coalesce(lion.feature_type_code, '0') AS "FeatureTyp",
        lion.segment_type AS "SegmentTyp",
        lion.incex_flag AS "IncExFlag",
        CASE
            WHEN lion.incex_flag = 'E' THEN 'N'
            WHEN lion.segment_type IN ('C', 'R', 'S', 'T') THEN 'R'
            WHEN lion.segment_type IN ('G', 'F') THEN 'G'
            ELSE 'B'
        END AS "RB_Layer",
        lion.nonped AS "NonPed",
        lion.traffic_direction AS "TrafDir",
        CASE lion.trafdir_ver_flag
            WHEN 'F' THEN 'FLD'
            WHEN 'P' THEN 'DCP'
            WHEN 'T' THEN 'DOT'
        END AS "TrafSrc",
        -- Overridden: SpecAddr on a replicant row is its own SAF type, not the base
        -- segment's (ETL spec §2.7.3).
        saf_replicants.specaddr AS "SpecAddr",
        lion.face_code AS "FaceCode",
        lion.segment_seqnum AS "SeqNum",
        lion.five_digit_street_code AS "StreetCode",
        saf_replicants.saf_street_code AS "SAFStreetCode",
        lion.lgc1 AS "LGC1",
        lion.lgc2 AS "LGC2",
        lion.lgc3 AS "LGC3",
        lion.lgc4 AS "LGC4",
        lion.lgc5 AS "LGC5",
        lion.lgc6 AS "LGC6",
        lion.lgc7 AS "LGC7",
        lion.lgc8 AS "LGC8",
        lion.lgc9 AS "LGC9",
        lion.boe_lgc_pointer AS "BOE_LGC",
        lpad(lion.segmentid::text, 7, '0') AS "SegmentID",
        -- Not adjusted for replication (see module docstring: SegCount should reflect
        -- all coincident records including SAF replicants, per spec - not implemented).
        lion.coincident_seg_count::text AS "SegCount",
        lion.segment_locational_status AS "LocStatus",
        -- Overridden only for AltSegmentData type D/F, side-gated; else the base zip.
        coalesce(saf_replicants.l_zip, lion.l_zip) AS "LZip",
        coalesce(saf_replicants.r_zip, lion.r_zip) AS "RZip",
        CASE WHEN lion.borough_boundary_indicator = 'L' THEN NULL ELSE lion.boroughcode::int END AS "LBoro",
        CASE WHEN lion.borough_boundary_indicator = 'R' THEN NULL ELSE lion.boroughcode::int END AS "RBoro",
        lion.left_community_district AS "L_CD",
        lion.right_community_district AS "R_CD",
        lion.left_dynamic_block AS "LATOMICPOLYGON",
        lion.right_dynamic_block AS "RATOMICPOLYGON",
        lion.left_2020_census_tract_basic::text AS "LCT2020",
        lion.left_2020_census_tract_suffix::text AS "LCT2020Suf",
        lion.right_2020_census_tract_basic::text AS "RCT2020",
        lion.right_2020_census_tract_suffix::text AS "RCT2020Suf",
        lion.left_2020_census_block_basic::text AS "LCB2020",
        lion.left_2020_census_block_suffix AS "LCB2020Suf",
        lion.right_2020_census_block_basic::text AS "RCB2020",
        lion.right_2020_census_block_suffix AS "RCB2020Suf",
        lion.left_2010_census_tract_basic::text AS "LCT2010",
        lion.left_2010_census_tract_suffix::text AS "LCT2010Suf",
        lion.right_2010_census_tract_basic::text AS "RCT2010",
        lion.right_2010_census_tract_suffix::text AS "RCT2010Suf",
        lion.left_2010_census_block_basic::text AS "LCB2010",
        lion.left_2010_census_block_suffix AS "LCB2010Suf",
        lion.right_2010_census_block_basic::text AS "RCB2010",
        lion.right_2010_census_block_suffix AS "RCB2010Suf",
        lion.left_2000_census_tract_basic::text AS "LCT2000",
        lion.left_2000_census_tract_suffix::text AS "LCT2000Suf",
        lion.right_2000_census_tract_basic::text AS "RCT2000",
        lion.right_2000_census_tract_suffix::text AS "RCT2000Suf",
        lion.left_2000_census_block_basic::text AS "LCB2000",
        lion.left_2000_census_block_suffix::text AS "LCB2000Suf",
        lion.right_2000_census_block_basic::text AS "RCB2000",
        lion.right_2000_census_block_suffix::text AS "RCB2000Suf",
        lion.left_1990_census_tract_basic::text AS "LCT1990",
        lion.left_1990_census_tract_suffix::text AS "LCT1990Suf",
        lion.right_1990_census_tract_basic::text AS "RCT1990",
        lion.right_1990_census_tract_suffix::text AS "RCT1990Suf",
        lion.left_assembly_district AS "LAssmDist",
        lion.left_election_district AS "LElectDist",
        lion.right_assembly_district AS "RAssmDist",
        lion.right_election_district AS "RElectDist",
        lion.split_election_district_flag AS "SplitElect",
        lion.left_school_district AS "LSchlDist",
        lion.right_school_district AS "RSchlDist",
        NULL::text AS "SplitSchl",
        lion.lsubsect AS "LSubSect",
        lion.rsubsect AS "RSubSect",
        lion.sandist_ind AS "SanDistInd",
        lion.from_sectionalmap AS "MapFrom",
        lion.to_sectionalmap AS "MapTo",
        lion.borough_boundary_indicator AS "BoroBndry",
        CASE
            WHEN
                lion.boroughcode::int = 1
                AND (
                    (lion.left_borocode::int = 1 AND lion.left_2000_census_tract_basic = 309)
                    OR (lion.right_borocode::int = 1 AND lion.right_2000_census_tract_basic = 309)
                )
                THEN 'M'
            WHEN
                lion.boroughcode::int = 2
                AND (
                    (lion.left_borocode::int = 2 AND lion.left_2000_census_tract_basic = 1)
                    OR (lion.right_borocode::int = 2 AND lion.right_2000_census_tract_basic = 1)
                )
                THEN 'R'
        END AS "MH_RI_Flag",
        lion.from_x AS "XFrom",
        lion.from_y AS "YFrom",
        lion.to_x AS "XTo",
        lion.to_y AS "YTo",
        lion.center_of_curvature_x::int AS "ArcCenterX",
        lion.center_of_curvature_y::int AS "ArcCenterY",
        lion.curve_flag AS "CurveFlag",
        NULL::int AS "Radius",
        lpad(lion.from_nodeid::text, 7, '0') AS "NodeIDFrom",
        lpad(lion.to_nodeid::text, 7, '0') AS "NodeIDTo",
        lion.from_level_code AS "NodeLevelF",
        lion.to_level_code AS "NodeLevelT",
        lion.continuous_parity_flag AS "ConParity",
        lion.twisted_parity_flag AS "Twisted",
        lion.rw_type::text AS "RW_TYPE",
        lion.physicalid::int AS "PhysicalID",
        lion.genericid::int AS "GenericID",
        lion.nypdid::text AS "NYPDID",
        lion.fdnyid AS "FDNYID",
        lion.l_blockfaceid::text AS "LBlockFaceID",
        lion.r_blockfaceid::text AS "RBlockFaceID",
        lpad(coalesce(lion.legacy_segmentid, 0)::text, 7, '0') AS "LegacyID",
        lion.status AS "Status",
        lion.streetwidth_min::float AS "StreetWidth_Min",
        lion.streetwidth_max::float AS "StreetWidth_Max",
        lion.streetwidth_irr AS "StreetWidth_Irr",
        coalesce(lion.bike_lane_1, ' ') || coalesce(lion.bike_lane_2, ' ') AS "BikeLane",
        lion.bike_traffic_direction AS "BIKE_TRAFDIR",
        lion.active_flag AS "ACTIVE_FLAG",
        lion.posted_speed::text AS "POSTED_SPEED",
        lion.snow_priority AS "Snow_Priority",
        lion.number_travel_lanes::text AS "Number_Travel_Lanes",
        lion.number_park_lanes::text AS "Number_Park_Lanes",
        lion.number_total_lanes::text AS "Number_Total_Lanes",
        lion.carto_display_level AS "Carto_Display_Level",
        lion.fcc AS "FCC",
        lion.right_of_way_type AS "ROW_Type",
        -- Overridden: house numbers are SAF-specific and side-gated on replicant rows
        -- (ETL spec §2.7.2/§2.7.3), not the base segment's own house numbers.
        saf_replicants.l_low_hn AS "LLo_Hyphen",
        saf_replicants.l_high_hn AS "LHi_Hyphen",
        saf_replicants.r_low_hn AS "RLo_Hyphen",
        saf_replicants.r_high_hn AS "RHi_Hyphen",
        NULL::int AS "FromLeft",
        NULL::int AS "ToLeft",
        NULL::int AS "FromRight",
        NULL::int AS "ToRight",
        -- Overridden: the SAF-replicant Join_ID encoding (ETL spec §2.7.3), computed in
        -- int__saf_replicant_fields.sql.
        saf_replicants.join_id AS "Join_ID",
        lion.left_nypd_service_area AS "L_PD_Service_Area",
        lion.right_nypd_service_area AS "R_PD_Service_Area",
        lion.truck_route_type AS "TRUCK_ROUTE_TYPE",
        st_length(lion.geom)::float AS "SHAPE_Length",
        lion.geom
    FROM lion
    INNER JOIN saf_replicants ON lion.lionkey = saf_replicants.lionkey
)

SELECT * FROM base_rows
UNION ALL
SELECT * FROM replicant_rows
