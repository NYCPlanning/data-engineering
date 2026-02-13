{{ config(
    materialized = 'view'
) }}
-- Analysis: MIH Areas Questionable Assignments
-- Purpose: Identify lots with "iffy" MIH area assignments due to low spatial overlap
-- 
-- This view shows lots where the MIH assignment is questionable due to:
-- 1. Low percentage of lot covered by MIH area (between 10-25%)
-- 2. Low percentage of MIH area covered by lot (between 50-75%)
-- 3. Edge cases that barely meet assignment thresholds
--
-- Unlike transit zones, multiple MIH areas can legitimately apply to a single lot.
-- We focus on identifying assignments with marginal spatial overlap that may need review.

WITH questionable_assignments AS (
    SELECT
        mlo.bbl,
        mlo.project_id,
        mlo.mih_id,
        mlo.cleaned_option,
        mlo.perbblgeom AS pct_lot_covered,
        mlo.maxpermihgeom AS pct_mih_covered,
        -- Calculate how "iffy" this assignment is (lower scores = more questionable)
        LEAST(
            CASE WHEN mlo.perbblgeom >= 10 THEN mlo.perbblgeom ELSE 0 END,
            CASE WHEN mlo.maxpermihgeom >= 50 THEN mlo.maxpermihgeom ELSE 0 END
        ) AS assignment_strength,
        p.geom AS lot_geom,
        p.address,
        p.zonedist1
    FROM mih_lot_overlap AS mlo
    LEFT JOIN pluto AS p ON mlo.bbl = p.bbl
    WHERE mlo.perbblgeom BETWEEN 10 AND 30
), assignment_context AS (
    -- Add basic context and geometry
    SELECT
        qa.*,
        mc.wkb_geometry AS mih_geom
    FROM questionable_assignments AS qa
    LEFT JOIN mih_cleaned AS mc ON qa.mih_id = mc.mih_id
)
SELECT
    bbl,
    project_id,
    cleaned_option,
    pct_lot_covered,
    pct_mih_covered,
    assignment_strength,
    address,
    zonedist1,
    lot_geom,
    mih_geom,
    ST_ENVELOPE(ST_BUFFER(lot_geom, 0.005)) AS area_of_interest_geom
FROM assignment_context
ORDER BY assignment_strength ASC, pct_lot_covered ASC
