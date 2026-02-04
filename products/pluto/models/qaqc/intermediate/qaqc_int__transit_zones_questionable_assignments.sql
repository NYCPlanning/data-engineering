{{ config(
    materialized = 'view'
) }}
-- This intended to be a convenience to visualize ambiguous assignments for transit zones
-- Each row will have a questionable block/lot, along with all relevant geometries so
-- you can see them overlaid.
-- 
-- This view shows:
-- 1. Blocks that have competing transit zone assignments (multiple zones with >10% coverage)
-- 2. Individual lots within those ambiguous blocks that will receive lot-level assignments
--
-- The flagged blocks will be subdivided into their constituent lots, and each lot will then
-- be assigned to its highest-coverage transit zone rather than using the block-level assignment.

WITH all_ambiguous_assignments AS (
    -- Union both block and lot rankings
    SELECT * FROM transit_zones_block_to_tz_ranked
    WHERE
        id IN (
            SELECT DISTINCT t.id
            FROM transit_zones_block_to_tz_ranked AS t
            WHERE t.tz_rank = 2 AND t.pct_covered >= 10
        )
    UNION ALL
    SELECT * FROM transit_zones_bbl_to_tz_ranked
    WHERE id IN (
        SELECT DISTINCT t.id
        FROM transit_zones_bbl_to_tz_ranked AS t
        WHERE t.tz_rank = 2 AND t.pct_covered >= 10
    )
),
winners_losers AS (
    SELECT
        max(100 - abs(50 - rk.pct_covered)) OVER (PARTITION BY rk.id) AS risk,
        rk.assignment_type,
        rk.id,
        rk.borough,
        rk.block,
        rk.bbls,
        rk.transit_zone AS winner_tz,
        rk2.transit_zone AS loser_tz,
        rk.pct_covered AS winner_pct,
        rk2.pct_covered AS loser_pct,
        -- blocks get geom from tax_blocks, lots get geom from pluto
        CASE
            WHEN rk.assignment_type = 'block' THEN tb.geom
            ELSE p.geom
        END AS geom,
        p.geom AS pluto_geom,
        CASE
            WHEN rk.assignment_type = 'block'
                THEN
                    (
                        SELECT array_agg(DISTINCT zonedist1)
                        FROM pluto AS p2
                        WHERE p2.borough = rk.borough AND p2.block = rk.block
                    )
            ELSE
                ARRAY[p.zonedist1]
        END AS block_zone_dists
    FROM all_ambiguous_assignments AS rk
    INNER JOIN all_ambiguous_assignments AS rk2 ON rk.id = rk2.id AND rk2.tz_rank = 2
    LEFT JOIN transit_zones_tax_blocks AS tb
        ON
            rk.assignment_type = 'block'
            AND rk.borough = tb.borough
            AND rk.block = tb.block
            AND rk.sub_block = tb.sub_block
    LEFT JOIN pluto AS p ON rk.assignment_type = 'lot' AND p.bbl = rk.bbls[1]
    WHERE rk.tz_rank = 1
)
SELECT
    wl.assignment_type,
    wl.geom AS block_geom,
    wl.risk,
    wl.winner_pct,
    wl.loser_pct,
    wl.id,
    wl.bbls,
    wl.borough,
    wl.block,
    wl.block_zone_dists,
    wl.winner_tz,
    wl.loser_tz,
    st_envelope(st_buffer(wl.geom, .005)) AS area_of_interest_geom,
    (
        SELECT st_intersection(st_envelope(st_buffer(wl.geom, .005)), wkb_geometry)
        FROM dcp_transit_zones AS dtz
        WHERE dtz.transit_zone = wl.winner_tz
    ) AS winner_tz_geom,
    (
        SELECT st_intersection(st_envelope(st_buffer(wl.geom, .005)), wkb_geometry)
        FROM dcp_transit_zones AS dtz
        WHERE dtz.transit_zone = wl.loser_tz
    ) AS loser_tz_geom
FROM winners_losers AS wl
WHERE wl.block_zone_dists != '{"PARK"}'
ORDER BY wl.assignment_type ASC, wl.risk DESC;
