DROP VIEW IF EXISTS mih_areas_cleaned_options;
CREATE VIEW mih_areas_cleaned_options AS
SELECT
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
FROM dcp_mih;

-- Distinct Options, for QA purposs
DROP VIEW IF EXISTS mih_areas_distinct_option;
CREATE VIEW mih_areas_distinct_option AS
SELECT DISTINCT p.split_value
FROM
    mih_areas_cleaned_options AS m
CROSS JOIN
    LATERAL
    unnest(string_to_array(m.cleaned_option, ',')) AS p (split_value)
ORDER BY p.split_value DESC;

-- Determine which MIH Areas apply to which tax lots
DROP TABLE IF EXISTS mihperorder CASCADE;
CREATE TABLE mihperorder AS
WITH mihper AS (
    SELECT
        p.bbl,
        m.project_id,
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
    INNER JOIN mih_areas_cleaned_options AS m
        ON st_intersects(p.geom, m.wkb_geometry)
),
mih_areas AS (
    SELECT
        bbl,
        cleaned_option,
        project_id,
        sum(segbblgeom) AS segbblgeom,
        sum(segmihgeom) AS segmihgeom,
        sum(segbblgeom / allbblgeom) * 100 AS perbblgeom,
        max(segmihgeom / allmihgeom) * 100 AS maxpermihgeom
    FROM mihper
    GROUP BY bbl, cleaned_option, project_id
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
    FROM mihperorder
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
