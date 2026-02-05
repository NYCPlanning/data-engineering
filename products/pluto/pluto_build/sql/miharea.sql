-- Mandatory Inclusionary Housing (MIH) Area Assignment Logic
-- 
-- Assign MIH affordability options to tax lots based on spatial overlap with MIH areas
--
-- Assignment Strategy:
-- Unlike transit zones where each lot gets assigned to exactly one zone, MIH areas can have
-- multiple overlapping affordability options that ALL apply to a single lot. A lot is assigned 
-- to a MIH option if either:
--   1. ≥10% of the lot area is covered by the MIH area, OR
--   2. ≥50% of the MIH area is covered by the lot
--
-- Multiple Options Per Lot:
-- A single lot can legitimately have multiple MIH options (e.g., Option 1, Option 2, Deep Affordability).
-- These are not competing assignments but rather cumulative policy options that apply to development
-- on that lot. The final output pivots these into binary flags (mih_opt1, mih_opt2, etc.).
--
-- Data Flow:
-- 1. Clean MIH option names and create unique identifiers (mih_cleaned table)
-- 2. Calculate spatial overlaps between lots and MIH areas (mih_lot_overlap table)
-- 3. Filter to assignments meeting the coverage thresholds
-- 4. Pivot multiple options per lot into binary columns on the pluto table


DROP TABLE IF EXISTS mih_cleaned;
CREATE TABLE mih_cleaned AS
SELECT
    project_id || '-' || mih_option AS mih_id,
    *,
    trim(
        -- Step 2b: collapse any sequence of commas (e.g., ",,", ",,,")
        regexp_replace(
        -- Step 2a: Replace "and" or "," (with any spaces) with a single comma
            regexp_replace(
                -- Step 1: Add space between "Option" and number
                regexp_replace(
                    replace(mih_option, 'Affordablility', 'Affordability'), -- should probably fix this in the source data
                    'Option(\d)',          -- ← match "Option" followed by a digit
                    'Option \1',           -- ← insert space
                    'g'
                ),
                '\s*(,|and)\s*',         -- ← match a comma or "and" (with spaces)
                ',',                     -- ← replace with a comma
                'g'
            ),
            ',+',                      -- ← match one or more commas in a row
            ',',                       -- ← replace with a single comma
            'g'
        ),
        ', '                        -- ← trim comma and space FROM start/end
    ) AS cleaned_option
FROM dcp_gis_mandatory_inclusionary_housing;


DROP TABLE IF EXISTS mih_lot_overlap CASCADE;
CREATE TABLE mih_lot_overlap AS
WITH mih_per_area AS (
    SELECT
        p.bbl,
        m.project_id,
        m.mih_id,
        m.wkb_geometry AS mih_geom,
        p.geom AS lot_geom,
        m.cleaned_option,
        st_area(
            CASE
                WHEN st_coveredby(p.geom, m.wkb_geometry) THEN p.geom
                ELSE st_multi(st_intersection(p.geom, m.wkb_geometry))
            END
        ) AS segbblgeom,
        st_area(p.geom) AS allbblgeom,
        st_area(
            CASE
                WHEN st_coveredby(m.wkb_geometry, p.geom) THEN m.wkb_geometry
                ELSE st_multi(st_intersection(m.wkb_geometry, p.geom))
            END
        ) AS segmihgeom,
        st_area(m.wkb_geometry) AS allmihgeom
    FROM pluto AS p
    INNER JOIN mih_cleaned AS m
        ON st_intersects(p.geom, m.wkb_geometry)
),
mih_areas AS (
    SELECT
        bbl,
        cleaned_option,
        project_id,
        mih_id,
        sum(segbblgeom) AS segbblgeom,
        sum(segmihgeom) AS segmihgeom,
        sum(segbblgeom / allbblgeom) * 100 AS perbblgeom,
        max(segmihgeom / allmihgeom) * 100 AS maxpermihgeom
    FROM mih_per_area
    GROUP BY bbl, cleaned_option, project_id, mih_id
)
SELECT * FROM mih_areas
WHERE perbblgeom >= 10 OR maxpermihgeom >= 50;


-- NOTE: GIS will likely refactor dcp_mih into this pivoted format,
-- so much this code will likely disappear.
--
-- Find all distinct MIH areas that apply to a lot, and pivot to columns.
-- e.g. if we have two rows from our geospatial join like so:
-- bbl=123, mih_options=Option 1,Option 2
-- bbl=123, mih_options=Option 2,Option 3
-- we first aggregate to
-- bbl=123, Option 1,Option 2,Option 2,Option 3
-- then pivot into distinct columns
WITH bbls_with_all_options AS (
    SELECT
        bbl,
        string_agg(cleaned_option, ',') AS all_options
    FROM mih_lot_overlap
    GROUP BY bbl
), pivoted AS (
    SELECT
        bbl,
        CASE
            WHEN (all_options LIKE '%Option 1%') = true THEN '1'
        END AS mih_opt1,
        CASE
            WHEN (all_options LIKE '%Option 2%') = true THEN '1'
        END AS mih_opt2,
        CASE
            WHEN (all_options LIKE '%Option 3%' OR all_options LIKE '%Deep Affordability Option%') = true THEN '1'
        END AS mih_opt3,
        CASE
            WHEN (all_options LIKE '%Deep Affordability Option%') = true THEN '1'
        END AS mih_opt4
    FROM bbls_with_all_options
)
UPDATE pluto
SET
    mih_opt1 = m.mih_opt1,
    mih_opt2 = m.mih_opt2,
    mih_opt3 = m.mih_opt3,
    mih_opt4 = m.mih_opt4
FROM pivoted AS m
WHERE pluto.bbl = m.bbl
