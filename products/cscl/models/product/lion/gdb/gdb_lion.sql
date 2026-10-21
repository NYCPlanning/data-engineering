{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Base LION rows (one per segment/proto-segment) plus SAF-replicant rows (one per SAF
-- datum associated with a segment - ETL spec §2.7.2 "Segment Replication for SAF Data").
-- A replicant has identical geometry/attributes to its base row except for SpecAddr,
-- SAFStreetName/SAFStreetCode (fields that only exist on replicants), house
-- numbers/zip, and Join_ID - see int__saf_replicant_fields.sql for the override values.
-- Continuous-parity segments and SAF entries are written as two rows (ETL spec §2.7.1).

WITH lion AS (
    SELECT * FROM {{ ref('int__lion') }}
    WHERE include_in_bytes_lion
),
saf_replicants AS (
    SELECT * FROM {{ ref('int__saf_replicant_fields') }}
),

-- SegCount (legacy GetCoincidentSegmentCount): the number of output records for the
-- segment (SAF replicants and continuous-parity copies included), plus one for each
-- AltSegmentData SAF record that isn't written out as a replicant (e.g. type Z) and for
-- each CommonPlace G/N/X record without a B7SC, plus
-- one for each other segment of the same feature class (centerline with centerline,
-- rail with rail, ...) with equal geometry; capped at 9 (a single character in prod).
-- Legacy tests equality within the source feature classes' XY tolerance (0.0002 ft, on a
-- 0.0001 ft resolution grid), so equal here means the same two nodes and each geometry
-- within var segcount_coincident_tolerance_ft of the other. Densified true curves differ
-- by float noise, so an exact geometry match misses some.
segment_geoms AS (
    SELECT
        segmentid,
        source_table,
        any_value(least(from_nodeid, to_nodeid)) AS node_a,
        any_value(greatest(from_nodeid, to_nodeid)) AS node_b,
        any_value(geom) AS geom
    FROM {{ ref('int__lion') }}
    WHERE
        source_table = 'centerline'
        OR (include_in_bytes_lion AND source_table IN ('rail', 'subway', 'nonstreetfeatures'))
    GROUP BY segmentid, source_table
),

coincident_segment_counts AS (
    SELECT
        segment.segmentid,
        count(DISTINCT other.segmentid) AS n_coincident
    FROM segment_geoms AS segment
    INNER JOIN segment_geoms AS other
        ON
            segment.source_table = other.source_table
            AND segment.node_a = other.node_a
            AND segment.node_b = other.node_b
            AND segment.segmentid <> other.segmentid
    WHERE
        st_covers(st_buffer(other.geom, {{ var('segcount_coincident_tolerance_ft') }}), segment.geom)
        AND st_covers(st_buffer(segment.geom, {{ var('segcount_coincident_tolerance_ft') }}), other.geom)
    GROUP BY segment.segmentid
),

segment_row_counts AS (
    SELECT
        per_record.segmentid,
        least(9, sum(per_record.n_rows) + coalesce(max(coincident_segment_counts.n_coincident), 0)) AS n_rows
    FROM (
        SELECT
            segmentid,
            CASE WHEN continuous_parity_flag IN ('L', 'R') THEN 2 ELSE 1 END AS n_rows
        FROM lion
        UNION ALL
        SELECT
            lion.segmentid,
            CASE WHEN saf_replicants.parity_side IS NOT NULL THEN 2 ELSE 1 END AS n_rows
        FROM lion
        INNER JOIN saf_replicants ON lion.lionkey = saf_replicants.lionkey
        UNION ALL
        SELECT
            lion.segmentid,
            1 AS n_rows
        FROM lion
        INNER JOIN {{ ref('int__saf_segments') }} AS saf_segments
            ON lion.lionkey = saf_segments.segment_lionkey
        WHERE
            saf_segments.saf_source_table = 'altsegmentdata'
            AND NOT EXISTS (
                SELECT 1 FROM saf_replicants
                WHERE
                    saf_replicants.saf_globalid = saf_segments.saf_globalid
                    AND saf_replicants.lionkey = saf_segments.segment_lionkey
            )
        UNION ALL
        -- CommonPlace G/N/X records without a B7SC are never written (int__saf_segments
        -- drops them), but legacy counts every SAF record, written or not
        SELECT
            lion.segmentid,
            1 AS n_rows
        FROM lion
        INNER JOIN {{ source('recipe_sources', 'dcp_cscl_commonplace_gdb') }} AS commonplace
            ON lion.segmentid = commonplace.segmentid
        WHERE
            lion.source_table IN ('centerline', 'shoreline')
            AND commonplace.saftype IN ('G', 'N', 'X')
            AND commonplace.b7sc IS NULL
            AND commonplace.security_level <> '3'
    ) AS per_record
    LEFT JOIN coincident_segment_counts ON per_record.segmentid = coincident_segment_counts.segmentid
    GROUP BY per_record.segmentid
),

base_rows AS (
    SELECT
        street AS "Street",
        repeat(' ', 32) AS "SAFStreetName",
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
        boroughcode::text || five_digit_street_code AS "StreetCode",
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
        lpad(lion.segmentid::text, 7, '0') AS "SegmentID",
        segment_row_counts.n_rows::text AS "SegCount",
        segment_locational_status AS "LocStatus",
        l_zip AS "LZip",
        r_zip AS "RZip",
        -- ETL spec §2.7.3 (BL29/30): the segment "belongs" to boroughcode. When BoroBndry
        -- marks one side as out-of-borough, that side's Boro field is null and the other
        -- side is populated with boroughcode instead of its own (out-of-borough) side.
        CASE WHEN borough_boundary_indicator = 'L' THEN NULL ELSE boroughcode::int END AS "LBoro",
        CASE WHEN borough_boundary_indicator = 'R' THEN NULL ELSE boroughcode::int END AS "RBoro",
        -- Community district '0' (atomic polygons outside any district, e.g. water) is
        -- written as blank in prod; prod never has a '0' L_CD/R_CD.
        nullif(left_community_district, '0') AS "L_CD",
        nullif(right_community_district, '0') AS "R_CD",
        left_dynamic_block AS "LATOMICPOLYGON",
        right_dynamic_block AS "RATOMICPOLYGON",
        lpad(left_2020_census_tract_basic::text, 4, ' ') AS "LCT2020",
        coalesce(lpad(left_2020_census_tract_suffix::text, 2, '0'), '  ') AS "LCT2020Suf",
        lpad(right_2020_census_tract_basic::text, 4, ' ') AS "RCT2020",
        coalesce(lpad(right_2020_census_tract_suffix::text, 2, '0'), '  ') AS "RCT2020Suf",
        lpad(left_2020_census_block_basic::text, 4, '0') AS "LCB2020",
        coalesce(left_2020_census_block_suffix::text, ' ') AS "LCB2020Suf",
        lpad(right_2020_census_block_basic::text, 4, '0') AS "RCB2020",
        coalesce(right_2020_census_block_suffix::text, ' ') AS "RCB2020Suf",
        lpad(left_2010_census_tract_basic::text, 4, ' ') AS "LCT2010",
        coalesce(lpad(left_2010_census_tract_suffix::text, 2, '0'), '  ') AS "LCT2010Suf",
        lpad(right_2010_census_tract_basic::text, 4, ' ') AS "RCT2010",
        coalesce(lpad(right_2010_census_tract_suffix::text, 2, '0'), '  ') AS "RCT2010Suf",
        lpad(left_2010_census_block_basic::text, 4, '0') AS "LCB2010",
        coalesce(left_2010_census_block_suffix::text, ' ') AS "LCB2010Suf",
        lpad(right_2010_census_block_basic::text, 4, '0') AS "RCB2010",
        coalesce(right_2010_census_block_suffix::text, ' ') AS "RCB2010Suf",
        lpad(left_2000_census_tract_basic::text, 4, ' ') AS "LCT2000",
        coalesce(lpad(left_2000_census_tract_suffix::text, 2, '0'), '  ') AS "LCT2000Suf",
        lpad(right_2000_census_tract_basic::text, 4, ' ') AS "RCT2000",
        coalesce(lpad(right_2000_census_tract_suffix::text, 2, '0'), '  ') AS "RCT2000Suf",
        lpad(left_2000_census_block_basic::text, 4, '0') AS "LCB2000",
        coalesce(left_2000_census_block_suffix::text, ' ') AS "LCB2000Suf",
        lpad(right_2000_census_block_basic::text, 4, '0') AS "RCB2000",
        coalesce(right_2000_census_block_suffix::text, ' ') AS "RCB2000Suf",
        lpad(left_1990_census_tract_basic::text, 4, ' ') AS "LCT1990",
        coalesce(lpad(left_1990_census_tract_suffix::text, 2, '0'), '  ') AS "LCT1990Suf",
        lpad(right_1990_census_tract_basic::text, 4, ' ') AS "RCT1990",
        coalesce(lpad(right_1990_census_tract_suffix::text, 2, '0'), '  ') AS "RCT1990Suf",
        left_assembly_district AS "LAssmDist",
        left_election_district AS "LElectDist",
        right_assembly_district AS "RAssmDist",
        right_election_district AS "RElectDist",
        split_election_district_flag AS "SplitElect",
        lpad(left_school_district, 2, '0') AS "LSchlDist",
        lpad(right_school_district, 2, '0') AS "RSchlDist",
        -- ETL spec §2.7.3 (BL62): "blank, it is one digit filler in Geosupport LION" - not
        -- a derived field. Confirmed 100% blank in prod (production_outputs.fgdb_lion).
        NULL::text AS "SplitSchl",
        lsubsect AS "LSubSect",
        rsubsect AS "RSubSect",
        sandist_ind AS "SanDistInd",
        rpad(coalesce(from_quarter_sectionalmap, ''), 3, ' ') AS "MapFrom",
        rpad(coalesce(to_quarter_sectionalmap, ''), 3, ' ') AS "MapTo",
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
        -- Prod's literal value for a non-curved segment is 0, not blank (confirmed
        -- against production_outputs.fgdb_lion: ArcCenterX/Y/Radius are 0 for every
        -- blank/'I'-CurveFlag row, never null) - coalesced at this output boundary
        -- only, same convention as FeatureTyp above (see CSCL-LION-07, data_issues.md).
        coalesce(center_of_curvature_x::int, 0) AS "ArcCenterX",
        coalesce(center_of_curvature_y::int, 0) AS "ArcCenterY",
        curve_flag AS "CurveFlag",
        coalesce(center_of_curvature_radius::int, 0) AS "Radius",
        lpad(from_nodeid::text, 7, '0') AS "NodeIDFrom",
        lpad(to_nodeid::text, 7, '0') AS "NodeIDTo",
        from_level_code AS "NodeLevelF",
        to_level_code AS "NodeLevelT",
        continuous_parity_flag AS "ConParity",
        twisted_parity_flag AS "Twisted",
        lpad(coalesce(rw_type::text, ''), 2, ' ') AS "RW_TYPE",
        physicalid::int AS "PhysicalID",
        genericid::int AS "GenericID",
        coalesce(lpad(nypdid::text, 7, '0'), repeat(' ', 7)) AS "NYPDID",
        fdnyid AS "FDNYID",
        coalesce(lpad(l_blockfaceid::text, 10, '0'), repeat(' ', 10)) AS "LBlockFaceID",
        coalesce(lpad(r_blockfaceid::text, 10, '0'), repeat(' ', 10)) AS "RBlockFaceID",
        -- Prod's convention for "no legacy ID" is a literal '0000000', not blank/null -
        -- coalesce before padding so segments without one match that instead of NULL.
        lpad(coalesce(legacy_segmentid, 0)::text, 7, '0') AS "LegacyID",
        status AS "Status",
        streetwidth_min_with_zeros::float AS "StreetWidth_Min",
        streetwidth_max::float AS "StreetWidth_Max",
        streetwidth_irr AS "StreetWidth_Irr",
        lpad(coalesce(bike_lane_2, ''), 2, ' ') AS "BikeLane",
        bike_traffic_direction AS "BIKE_TRAFDIR",
        -- ETL spec §2.7.3 (BL113): "Subway Active Flag" - only populated for Subway-sourced
        -- segments; null for Centerline/Rail.
        active_flag AS "ACTIVE_FLAG",
        posted_speed::text AS "POSTED_SPEED",
        snow_priority AS "Snow_Priority",
        lpad(coalesce(number_travel_lanes::text, ''), 2, ' ') AS "Number_Travel_Lanes",
        lpad(coalesce(number_park_lanes::text, ''), 2, ' ') AS "Number_Park_Lanes",
        lpad(coalesce(number_total_lanes::text, ''), 2, ' ') AS "Number_Total_Lanes",
        carto_display_level AS "Carto_Display_Level",
        fcc AS "FCC",
        right_of_way_type AS "ROW_Type",
        -- ETL spec §2.7.3 (BL101-104), House numbers: see lion_house_numbers.sql. Continuous
        -- parity segments (ETL spec §2.7.1) come out as two rows (hn.n = 1, 2); segments the
        -- legacy tool zeroes (generic/non-physical) come out empty.
        lpad(hn.l_low, 7, ' ') AS "LLo_Hyphen",
        lpad(hn.l_high, 7, ' ') AS "LHi_Hyphen",
        lpad(hn.r_low, 7, ' ') AS "RLo_Hyphen",
        lpad(hn.r_high, 7, ' ') AS "RHi_Hyphen",
        {{ lion_normalized_house_number('hn.l_low') }} AS "FromLeft",
        {{ lion_normalized_house_number('hn.l_high') }} AS "ToLeft",
        {{ lion_normalized_house_number('hn.r_low') }} AS "FromRight",
        {{ lion_normalized_house_number('hn.r_high') }} AS "ToRight",
        -- Join_ID links each segment to its AltNames entries (ETL spec §2.7.3). Non-SAF
        -- form; SAF replicants (below) use a different encoding.
        {{ lion_join_id() }} AS "Join_ID",
        left_nypd_service_area AS "L_PD_Service_Area",
        right_nypd_service_area AS "R_PD_Service_Area",
        truck_route_type AS "TRUCK_ROUTE_TYPE",
        st_length(geom)::float AS "SHAPE_Length",
        geom
    FROM lion
    INNER JOIN segment_row_counts ON lion.segmentid = segment_row_counts.segmentid
    CROSS JOIN
        LATERAL generate_series(
            1, CASE WHEN lion.continuous_parity_flag IN ('L', 'R') THEN 2 ELSE 1 END
        ) AS rng (n)
    CROSS JOIN LATERAL (
        {{ lion_house_number_split(
            "nullif(lion.l_low_hn, '0')", "nullif(lion.l_high_hn, '0')",
            "nullif(lion.r_low_hn, '0')", "nullif(lion.r_high_hn, '0')",
            'lion.continuous_parity_flag', 'rng.n',
            "(lion.segment_type NOT IN ('G', 'F') AND coalesce(lion.feature_type_code, '0') IN ('0', '6', 'W'))"
        ) }}
    ) AS hn
),

replicant_rows AS (
    SELECT
        lion.street AS "Street",
        rpad(coalesce(saf_replicants.saf_street_name, ''), 32, ' ') AS "SAFStreetName",
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
        lion.boroughcode::text || lion.five_digit_street_code AS "StreetCode",
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
        segment_row_counts.n_rows::text AS "SegCount",
        lion.segment_locational_status AS "LocStatus",
        -- AltSegmentData type D/F: zip only on the side with the SAF's house numbers, the
        -- other side stays blank (as in prod); other replicants take the base zip.
        CASE
            WHEN saf_replicants.specaddr IN ('D', 'F') THEN saf_replicants.l_zip
            ELSE coalesce(saf_replicants.l_zip, lion.l_zip)
        END AS "LZip",
        CASE
            WHEN saf_replicants.specaddr IN ('D', 'F') THEN saf_replicants.r_zip
            ELSE coalesce(saf_replicants.r_zip, lion.r_zip)
        END AS "RZip",
        CASE WHEN lion.borough_boundary_indicator = 'L' THEN NULL ELSE lion.boroughcode::int END AS "LBoro",
        CASE WHEN lion.borough_boundary_indicator = 'R' THEN NULL ELSE lion.boroughcode::int END AS "RBoro",
        nullif(lion.left_community_district, '0') AS "L_CD",
        nullif(lion.right_community_district, '0') AS "R_CD",
        lion.left_dynamic_block AS "LATOMICPOLYGON",
        lion.right_dynamic_block AS "RATOMICPOLYGON",
        lpad(lion.left_2020_census_tract_basic::text, 4, ' ') AS "LCT2020",
        coalesce(lpad(lion.left_2020_census_tract_suffix::text, 2, '0'), '  ') AS "LCT2020Suf",
        lpad(lion.right_2020_census_tract_basic::text, 4, ' ') AS "RCT2020",
        coalesce(lpad(lion.right_2020_census_tract_suffix::text, 2, '0'), '  ') AS "RCT2020Suf",
        lpad(lion.left_2020_census_block_basic::text, 4, '0') AS "LCB2020",
        coalesce(lion.left_2020_census_block_suffix::text, ' ') AS "LCB2020Suf",
        lpad(lion.right_2020_census_block_basic::text, 4, '0') AS "RCB2020",
        coalesce(lion.right_2020_census_block_suffix::text, ' ') AS "RCB2020Suf",
        lpad(lion.left_2010_census_tract_basic::text, 4, ' ') AS "LCT2010",
        coalesce(lpad(lion.left_2010_census_tract_suffix::text, 2, '0'), '  ') AS "LCT2010Suf",
        lpad(lion.right_2010_census_tract_basic::text, 4, ' ') AS "RCT2010",
        coalesce(lpad(lion.right_2010_census_tract_suffix::text, 2, '0'), '  ') AS "RCT2010Suf",
        lpad(lion.left_2010_census_block_basic::text, 4, '0') AS "LCB2010",
        coalesce(lion.left_2010_census_block_suffix::text, ' ') AS "LCB2010Suf",
        lpad(lion.right_2010_census_block_basic::text, 4, '0') AS "RCB2010",
        coalesce(lion.right_2010_census_block_suffix::text, ' ') AS "RCB2010Suf",
        lpad(lion.left_2000_census_tract_basic::text, 4, ' ') AS "LCT2000",
        coalesce(lpad(lion.left_2000_census_tract_suffix::text, 2, '0'), '  ') AS "LCT2000Suf",
        lpad(lion.right_2000_census_tract_basic::text, 4, ' ') AS "RCT2000",
        coalesce(lpad(lion.right_2000_census_tract_suffix::text, 2, '0'), '  ') AS "RCT2000Suf",
        lpad(lion.left_2000_census_block_basic::text, 4, '0') AS "LCB2000",
        coalesce(lion.left_2000_census_block_suffix::text, ' ') AS "LCB2000Suf",
        lpad(lion.right_2000_census_block_basic::text, 4, '0') AS "RCB2000",
        coalesce(lion.right_2000_census_block_suffix::text, ' ') AS "RCB2000Suf",
        lpad(lion.left_1990_census_tract_basic::text, 4, ' ') AS "LCT1990",
        coalesce(lpad(lion.left_1990_census_tract_suffix::text, 2, '0'), '  ') AS "LCT1990Suf",
        lpad(lion.right_1990_census_tract_basic::text, 4, ' ') AS "RCT1990",
        coalesce(lpad(lion.right_1990_census_tract_suffix::text, 2, '0'), '  ') AS "RCT1990Suf",
        lion.left_assembly_district AS "LAssmDist",
        lion.left_election_district AS "LElectDist",
        lion.right_assembly_district AS "RAssmDist",
        lion.right_election_district AS "RElectDist",
        lion.split_election_district_flag AS "SplitElect",
        lpad(lion.left_school_district, 2, '0') AS "LSchlDist",
        lpad(lion.right_school_district, 2, '0') AS "RSchlDist",
        NULL::text AS "SplitSchl",
        lion.lsubsect AS "LSubSect",
        lion.rsubsect AS "RSubSect",
        lion.sandist_ind AS "SanDistInd",
        rpad(coalesce(lion.from_quarter_sectionalmap, ''), 3, ' ') AS "MapFrom",
        rpad(coalesce(lion.to_quarter_sectionalmap, ''), 3, ' ') AS "MapTo",
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
        coalesce(lion.center_of_curvature_x::int, 0) AS "ArcCenterX",
        coalesce(lion.center_of_curvature_y::int, 0) AS "ArcCenterY",
        lion.curve_flag AS "CurveFlag",
        coalesce(lion.center_of_curvature_radius::int, 0) AS "Radius",
        lpad(lion.from_nodeid::text, 7, '0') AS "NodeIDFrom",
        lpad(lion.to_nodeid::text, 7, '0') AS "NodeIDTo",
        lion.from_level_code AS "NodeLevelF",
        lion.to_level_code AS "NodeLevelT",
        lion.continuous_parity_flag AS "ConParity",
        lion.twisted_parity_flag AS "Twisted",
        lpad(coalesce(lion.rw_type::text, ''), 2, ' ') AS "RW_TYPE",
        lion.physicalid::int AS "PhysicalID",
        lion.genericid::int AS "GenericID",
        coalesce(lpad(lion.nypdid::text, 7, '0'), repeat(' ', 7)) AS "NYPDID",
        lion.fdnyid AS "FDNYID",
        coalesce(lpad(lion.l_blockfaceid::text, 10, '0'), repeat(' ', 10)) AS "LBlockFaceID",
        coalesce(lpad(lion.r_blockfaceid::text, 10, '0'), repeat(' ', 10)) AS "RBlockFaceID",
        lpad(coalesce(lion.legacy_segmentid, 0)::text, 7, '0') AS "LegacyID",
        lion.status AS "Status",
        lion.streetwidth_min_with_zeros::float AS "StreetWidth_Min",
        lion.streetwidth_max::float AS "StreetWidth_Max",
        lion.streetwidth_irr AS "StreetWidth_Irr",
        lpad(coalesce(lion.bike_lane_2, ''), 2, ' ') AS "BikeLane",
        lion.bike_traffic_direction AS "BIKE_TRAFDIR",
        lion.active_flag AS "ACTIVE_FLAG",
        lion.posted_speed::text AS "POSTED_SPEED",
        lion.snow_priority AS "Snow_Priority",
        lpad(coalesce(lion.number_travel_lanes::text, ''), 2, ' ') AS "Number_Travel_Lanes",
        lpad(coalesce(lion.number_park_lanes::text, ''), 2, ' ') AS "Number_Park_Lanes",
        lpad(coalesce(lion.number_total_lanes::text, ''), 2, ' ') AS "Number_Total_Lanes",
        lion.carto_display_level AS "Carto_Display_Level",
        lion.fcc AS "FCC",
        lion.right_of_way_type AS "ROW_Type",
        -- Overridden: house numbers are SAF-specific and side-gated on replicant rows
        -- (ETL spec §2.7.2/§2.7.3), not the base segment's own house numbers.
        lpad(hn.l_low, 7, ' ') AS "LLo_Hyphen",
        lpad(hn.l_high, 7, ' ') AS "LHi_Hyphen",
        lpad(hn.r_low, 7, ' ') AS "RLo_Hyphen",
        lpad(hn.r_high, 7, ' ') AS "RHi_Hyphen",
        {{ lion_normalized_house_number('hn.l_low') }} AS "FromLeft",
        {{ lion_normalized_house_number('hn.l_high') }} AS "ToLeft",
        {{ lion_normalized_house_number('hn.r_low') }} AS "FromRight",
        {{ lion_normalized_house_number('hn.r_high') }} AS "ToRight",
        -- Overridden: the SAF-replicant Join_ID encoding (ETL spec §2.7.3), computed in
        -- int__saf_replicant_fields.sql.
        saf_replicants.join_id AS "Join_ID",
        lion.left_nypd_service_area AS "L_PD_Service_Area",
        lion.right_nypd_service_area AS "R_PD_Service_Area",
        lion.truck_route_type AS "TRUCK_ROUTE_TYPE",
        st_length(lion.geom)::float AS "SHAPE_Length",
        lion.geom
    FROM lion
    -- skip a SAF record legacy doesn't write (still counted in SegCount above)
    INNER JOIN saf_replicants ON lion.lionkey = saf_replicants.lionkey AND saf_replicants.is_written
    INNER JOIN segment_row_counts ON lion.segmentid = segment_row_counts.segmentid
    CROSS JOIN LATERAL generate_series(1, CASE WHEN saf_replicants.parity_side IS NOT NULL THEN 2 ELSE 1 END) AS rng (n)
    CROSS JOIN LATERAL (
        {{ lion_house_number_split(
            'saf_replicants.l_low_hn', 'saf_replicants.l_high_hn',
            'saf_replicants.r_low_hn', 'saf_replicants.r_high_hn',
            'saf_replicants.parity_side', 'rng.n',
            "(lion.segment_type NOT IN ('G', 'F') AND coalesce(lion.feature_type_code, '0') IN ('0', '6', 'W'))"
        ) }}
    ) AS hn
)

SELECT * FROM base_rows
UNION ALL
SELECT * FROM replicant_rows
