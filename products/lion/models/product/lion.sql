WITH centerline AS (
    SELECT * FROM {{ ref("stg__centerline") }}
),

atomic_polygons AS (
    SELECT * FROM {{ ref("int__centerline_atomicpolygons") }}
)

SELECT
    centerline.boroughcode AS "Borough",
    NULL AS "Face Code",
    centerline.segment_seqnum AS "Sequence Number",
    centerline.segmentid AS "Segment ID",
    NULL AS "5-Digit Street Code (5SC)",
    NULL AS "LGC1",
    NULL AS "LGC2",
    NULL AS "LGC3",
    NULL AS "LGC4",
    NULL AS "Board of Elections LGC Pointer",
    NULL AS "From-Sectional Map",
    NULL AS "From-Node ID",
    NULL AS "From-X Coordinate",
    NULL AS "From-Y Coordinate",
    NULL AS "To-Sectional Map",
    NULL AS "To-Node ID",
    NULL AS "To-X Coordinate",
    NULL AS "To-Y Coordinate",
    ap.left_2000_census_tract AS "Left 2000 Census Tract",
    ap.left_atomicid AS "Left Dynamic Block", -- TODO: "last 3 bytes"
    centerline.l_low_hn AS "Left Low House Number",
    centerline.l_high_hn AS "Left High House Number",
    centerline.lsubsect AS "Left Dept of Sanitation Subsection", -- TODO: only 2 leftmost bytes
    centerline.l_zip AS "Left Zip Code",
    ap.left_assembly_district AS "Left Assembly District",
    ap.left_election_district AS "Left Election District",
    ap.left_school_district AS "Left School District",
    ap.right_2000_census_tract AS "Right 2000 Census Tract",
    ap.right_atomicid AS "Right Dynamic Block", -- TODO: "last 3 bytes"
    centerline.r_low_hn AS "Right Low House Number",
    centerline.r_high_hn AS "Right High House Number",
    centerline.rsubsect AS "Right Dept of Sanitation Subsection", -- TODO: only 2 leftmost bytes
    centerline.r_zip AS "Right Zip Code",
    ap.right_assembly_district AS "Right Assembly District",
    ap.right_election_district AS "Right Election District",
    ap.right_school_district AS "Right School District",
    NULL AS "Split Election District Flag",
    NULL AS "Filler (formerly Split Community School District Flag)", -- single space on export
    centerline.sandist_ind AS "Sanitation District Boundary Indicator",
    NULL AS "Traffic Direction",
    NULL AS "Segment Locational Status",
    NULL AS "Feature Type Code",
    centerline.nonped AS "Non-Pedestrian Flag",
    CASE
        WHEN centerline.continuous_parity_flag = '1' THEN 'L'
        WHEN centerline.continuous_parity_flag = '2' THEN 'R'
    END AS "Continuous Parity Indicator", -- on export, NULL should be a space
    NULL AS "Filler (formerly the Near BQ-Boundary Flag)", -- single space on export
    NULL AS "Borough Boundary Indicator",
    NULL AS "Twisted Parity Flag",
    NULL AS "Special Address Flag",
    NULL AS "Curve Flag",
    NULL AS "Center of Curvature X-Coordinate",
    NULL AS "Center of Curvature Y-Coordinate",
    round(centerline.shape_length) AS "Segment Length in Feet",
    NULL AS "From Level Code",
    NULL AS "To Level Code",
    centerline.trafdir_ver_flag AS "Traffic Direction Verification Flag",
    centerline.segment_type AS "Segment Type Code",
    centerline.coincident_seg_count AS "Coincident Segment Counter",
    centerline.incex_flag AS "Include/Exclude Flag",
    centerline.rw_type AS "Roadway Type",
    centerline.physicalid AS "PHYSICALID",
    centerline.genericid AS "GENERICID",
    centerline.nypdid AS "NYPDID",
    centerline.fdnyid AS "FDNYID",
    centerline.l_blockfaceid AS "Left BLOCKFACEID",
    centerline.r_blockfaceid AS "Right BLOCKFACEID",
    centerline.status AS "STATUS",
    centerline.streetwidth AS "Street Width",
    centerline.streetwidth_irr AS "Irregular Street Width Flag",
    centerline.bike_lane AS "Bike Lane Indicator",
    centerline.fcc AS "FCC",
    NULL AS "Right of Way Type", -- blank
    ap.left_2010_census_tract AS "Left 2010 Census Tract",
    ap.right_2010_census_tract AS "Right 2010 Census Tract",
    -- TODO: 2020 census tracts?
    NULL AS "LGC5",
    NULL AS "LGC6",
    NULL AS "LGC7",
    NULL AS "LGC8",
    NULL AS "LGC9",
    centerline.legacy_segmentid AS "Legacy SEGMENTID"
FROM centerline
LEFT JOIN atomic_polygons AS ap ON centerline.segmentid = ap.segmentid
