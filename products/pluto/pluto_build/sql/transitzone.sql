-- Transit zone assignment logic:
-- Determine whether > 10% of a lot is covered by one of the transit zones

-- A note on strategy:
-- The Transit Zone polygons are complex (high number of points) and each of the five zones
-- is a multipolygon containing multiple atomic parts distributed all over the map.
-- e.g. The Inner Transit Zone is comprised of polygon parts in Manhattan, Brooklyn and Queens.
--
-- To make the geospatial join of lot->Zone performant, we must expand these zones into atomic parts.
-- However the real performance killer is area calculations: naively calculating the area of PLUTO lots
-- in their respective zones takes a long time. (AR Note: I don't know for sure how long it takes - I killed it after 10 minutes)
-- So to calculate areas/pcts we must subdivide the Transit Zone atomic parts into simple geometries, then sum up
-- those pcts in the case that a lot geom is in multiple simple/subdivided parts. This approach should take ~1 minute.


-- Create decomposed transit zones table (break multipolygons into individual parts)
DROP TABLE IF EXISTS transit_zones_atomic_geoms;
CREATE TABLE transit_zones_atomic_geoms AS
WITH decomposed AS (
    SELECT
        transit_zone,
        (ST_DUMP(wkb_geometry)).geom AS wkb_geometry
    FROM dcp_transit_zones
)
SELECT
    transit_zone,
    wkb_geometry,
    ROW_NUMBER() OVER (ORDER BY transit_zone) AS decomposed_id
FROM decomposed;
CREATE INDEX idx_transit_zones_atomic_geoms_gix ON transit_zones_atomic_geoms USING gist (wkb_geometry);

-- Create subdivided transit zones table for optimal area calculation.
DROP TABLE IF EXISTS transit_zones_atomic_subdivided;
CREATE TABLE transit_zones_atomic_subdivided AS
SELECT
    transit_zone,
    decomposed_id,
    ST_SUBDIVIDE(wkb_geometry, 100) AS wkb_geometry  -- Max 100 vertices per piece
FROM transit_zones_atomic_geoms;
CREATE INDEX idx_transit_zones_atomic_subdivided_gix ON transit_zones_atomic_subdivided USING gist (wkb_geometry);


-- Step 1: Calculate coverage percentages for all lots using subdivisions of
-- the atomic transit zone parts
DROP TABLE IF EXISTS transit_zones_bbls_to_tz_atomic_parts;
CREATE TABLE transit_zones_bbls_to_tz_atomic_parts AS
WITH coverage AS (
    SELECT
        p.bbl,
        p.geom AS lot_geom,
        t.transit_zone,
        t.decomposed_id,
        ST_AREA(ST_INTERSECTION(p.geom, t.wkb_geometry)) / ST_AREA(p.geom) * 100.0 AS pct_covered
    FROM pluto AS p
    INNER JOIN transit_zones_atomic_subdivided AS t
        ON ST_INTERSECTS(p.geom, t.wkb_geometry)
)
SELECT
    bbl,
    transit_zone,
    lot_geom,
    decomposed_id,
    SUM(pct_covered) AS pct_covered
FROM coverage
GROUP BY bbl, transit_zone, lot_geom, decomposed_id;

-- Step 2: Aggregate by transit zone and rank
DROP TABLE IF EXISTS transit_zones_bbl_to_tz_ranked;
CREATE TABLE transit_zones_bbl_to_tz_ranked AS
WITH zone_totals AS (
    SELECT
        bbl,
        transit_zone,
        SUM(pct_covered) AS total_pct_covered
    FROM transit_zones_bbls_to_tz_atomic_parts
    GROUP BY bbl, transit_zone
)
SELECT
    bbl,
    transit_zone,
    total_pct_covered AS pct_covered,
    ROW_NUMBER() OVER (
        PARTITION BY bbl
        ORDER BY total_pct_covered DESC
    ) AS row_number
FROM zone_totals
WHERE total_pct_covered >= 10;
ANALYZE transit_zones_bbl_to_tz_ranked;

-- Assign the primary transit zone to each lot
UPDATE pluto a
SET trnstzone = b.transit_zone
FROM transit_zones_bbl_to_tz_ranked AS b
WHERE
    a.bbl = b.bbl
    AND row_number = 1;

-- Clean up intermediate tables that won't be used in QAQC
DROP TABLE IF EXISTS transit_zones_atomic_subdivided;
