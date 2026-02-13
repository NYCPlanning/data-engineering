-- Transit zone assignment logic:
-- Determine whether > 10% of a lot is covered by one of the transit zones

-- PERFORMANCE STRATEGY:
-- The Transit Zone polygons are complex multipolygons with parts distributed across the city.
-- For example, the Inner Transit Zone includes discontiguous polygon parts in Manhattan, Brooklyn and Queens.
--
-- CRITICAL: We must split these transit zones into their contiguous atomic parts before performing
-- spatial calculations. Without this decomposition, area calculations become prohibitively slow
-- (>10 minutes). By first splitting the zones with ST_DUMP, then summing the intersections back up
-- when calculating coverage percentages, we reduce processing time to ~1 minute.

-- ASSIGNMENT LOGIC:
-- We assign transit zones using a two-tier strategy:
--
-- 1. BLOCK-LEVEL ASSIGNMENT (when unambiguous):
--    - For tax blocks where a single transit zone clearly dominates (no competing zones >10% coverage),
--      assign all lots in that block to the dominant zone.
--    - This captures cases where a transit zone boundary might cut through individual lots within
--      a block, but we want those lots assigned consistently with their parent block.
--
-- 2. LOT-LEVEL ASSIGNMENT (when ambiguous):
--    - This is a fallback for when we can't determine a clear block-level assignment.
--    - For example, when a rail line and transit zone boundary cuts straight through a "block", creating ambiguity about
--      which transit zone the block belongs to. In such cases, we calculate coverage for each
--      individual lot and assign based on the lot's specific overlap.
--    - Note: We're not doing any complicated block geometry splitting here, just falling back to
--      lot-by-lot calculation when block-level assignment would probably be wrong.

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


-- Create the block geoms, splitting non-contiguous blocks into sub-blocks
-- and assigning lots to their sub-block
--
-- AR Note: I tried a few approaches for this, and perhaps there's a more clever/performant
-- way to accomplish this. Unfortunately, the recommend approach of ST_ClusterDBSCAN
-- will `sometimes` accomplish this, but it errors out seemingly randomly.
DROP TABLE IF EXISTS transit_zones_tax_blocks;
CREATE TABLE transit_zones_tax_blocks AS
WITH block_unions AS (
    SELECT
        borough,
        block,
        ST_UNION(p.geom) AS geom,
        ARRAY_AGG(bbl) AS all_bbls
    FROM pluto AS p
    GROUP BY p.borough, p.block
), block_parts AS (
    SELECT
        borough,
        block,
        all_bbls,
        (ST_DUMP(geom)).geom AS geom
    FROM block_unions
), numbered_parts AS (
    SELECT
        borough,
        block,
        all_bbls,
        geom,
        ROW_NUMBER() OVER (PARTITION BY borough, block ORDER BY ST_AREA(geom) DESC) AS sub_block
    FROM block_parts
), reassigned_bbls AS (
    SELECT
        np.borough,
        np.block,
        np.sub_block,
        np.geom,
        ARRAY_AGG(p.bbl) AS bbls
    FROM numbered_parts AS np
    INNER JOIN pluto AS p
        ON
            np.borough = p.borough
            AND np.block = p.block
            AND ST_WITHIN(ST_CENTROID(p.geom), np.geom)
    GROUP BY np.borough, np.block, np.sub_block, np.geom
)
SELECT
    borough,
    block,
    sub_block,
    borough || '-' || block || '-' || sub_block AS block_id,
    geom,
    bbls
FROM reassigned_bbls;
CREATE INDEX idx_transit_zones_tax_blocks_geom ON transit_zones_tax_blocks USING gist (geom);


-- Step 1: Calculate coverage percentages for all tax blocks
DROP TABLE IF EXISTS transit_zones_block_to_tz_ranked;
CREATE TABLE transit_zones_block_to_tz_ranked AS
WITH block_to_tz AS (
    SELECT
        tb.borough,
        tb.block,
        tb.sub_block,
        tb.geom,
        tb.bbls,
        t.transit_zone,
        -- determine how much of the block is covered by the transit zone (sum up area of all intersecting atomic parts, then divide by block area)
        ST_AREA(ST_INTERSECTION(tb.geom, ST_UNION(t.wkb_geometry))) / ST_AREA(tb.geom) * 100.0 AS pct_covered
    FROM transit_zones_tax_blocks AS tb
    INNER JOIN transit_zones_atomic_geoms AS t
        ON ST_INTERSECTS(tb.geom, t.wkb_geometry)
    GROUP BY tb.borough, tb.block, tb.sub_block, tb.geom, tb.bbls, t.transit_zone
)
SELECT
    'block' AS assignment_type,
    borough || '-' || block || '-' || sub_block AS id,
    borough,
    block,
    geom,
    sub_block,
    bbls,
    transit_zone,
    pct_covered,
    ROW_NUMBER() OVER (
        PARTITION BY borough, block, sub_block
        ORDER BY pct_covered DESC
    ) AS tz_rank
FROM block_to_tz;
ANALYZE transit_zones_block_to_tz_ranked;


-- For ambiguous blocks (those with competing transit zones), create lot-level assignments
DROP TABLE IF EXISTS transit_zones_bbl_to_tz_ranked;
CREATE TABLE transit_zones_bbl_to_tz_ranked AS
WITH ambiguous_bbls AS (
    SELECT
        UNNEST(bbls) AS bbl,
        borough,
        block,
        sub_block
    FROM transit_zones_block_to_tz_ranked AS tza
    WHERE tza.tz_rank > 1 AND tza.pct_covered > 10
), lot_to_tz AS (
    SELECT
        p.bbl,
        p.borough,
        p.block,
        t.transit_zone,
        p.geom,
        -- Calculate how much of the lot is covered by the transit zone
        ST_AREA(ST_INTERSECTION(p.geom, ST_UNION(t.wkb_geometry))) / ST_AREA(p.geom) * 100.0 AS pct_covered
    FROM pluto AS p
    INNER JOIN ambiguous_bbls AS ab ON p.bbl = ab.bbl
    INNER JOIN transit_zones_atomic_geoms AS t
        ON ST_INTERSECTS(p.geom, t.wkb_geometry)
    GROUP BY p.bbl, p.borough, p.block, t.transit_zone, p.geom
)
SELECT
    'lot' AS assignment_type,
    bbl::text AS id,
    borough,
    block,
    geom,
    1 AS sub_block,
    ARRAY[bbl] AS bbls,
    transit_zone,
    pct_covered,
    ROW_NUMBER() OVER (
        PARTITION BY bbl
        ORDER BY pct_covered DESC
    ) AS tz_rank
FROM lot_to_tz;
ANALYZE transit_zones_bbl_to_tz_ranked;

-- Assign the primary transit zone by
-- 1. tax block, when the block's tz assignment is not ambiguous
-- 2. by lot, even when ambiguous. We'll use corrections afterwards, if necessary

-- Assign transit zones using both block-level and lot-level strategies
UPDATE pluto a
SET trnstzone = assignments.transit_zone
FROM (
    -- Block-level assignments for non-ambiguous blocks
    SELECT
        UNNEST(bbls) AS bbl,
        transit_zone
    FROM transit_zones_block_to_tz_ranked AS block_tz
    WHERE
        block_tz.tz_rank = 1
        -- Only assign blocks that are not ambiguous (no second-ranked transit zone)
        AND NOT EXISTS (
            SELECT 1 FROM transit_zones_block_to_tz_ranked AS ambiguous
            WHERE
                ambiguous.borough = block_tz.borough
                AND ambiguous.block = block_tz.block
                AND ambiguous.tz_rank = 2
        )

    UNION ALL

    -- Lot-level assignments for ambiguous blocks
    SELECT
        bbls[1] AS bbl,
        transit_zone
    FROM transit_zones_bbl_to_tz_ranked
    WHERE tz_rank = 1
) AS assignments
WHERE a.bbl = assignments.bbl;
