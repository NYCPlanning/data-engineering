{{ config(
    materialized = 'table'
) }}
-- Analysis: Transit Zone Questionable Assignments
-- Purpose: Identify tax lots with multiple transit zone assignments and analyze coverage patterns
--
-- This analysis identifies BBLs that intersect with multiple transit zones
-- and pulls in the relevant geometries for analysis puprposes.

WITH bbls_with_multiple_zones AS (
    -- Find BBLs that overlap with more than one transit zone
    SELECT bbl
    FROM {{ source("build_sources", "transit_zones_bbls_to_tz_atomic_parts") }}
    GROUP BY bbl
    HAVING count(DISTINCT transit_zone) > 1
),

transit_zones_coverage_analysis AS (
    -- Calculate coverage metrics for BBLs with multiple zone assignments
    SELECT
        t.bbl,
        t.transit_zone,
        t.pct_covered,
        td.wkb_geometry AS zone_decomposed_geometry,
        p.geom AS lot_geometry,
        t.lot_geom AS lot_clipped_geometry,
        -- Calculate how close the coverage percentage is to 50% (most ambiguous case)
        50 - abs(t.pct_covered - 50) AS ambiguity_score,
        -- Get the maximum ambiguity for a bbl
        max(50 - abs(t.pct_covered - 50)) OVER (PARTITION BY t.bbl) AS max_bbl_ambiguity_score
    FROM {{ source("build_sources", "transit_zones_bbls_to_tz_atomic_parts") }} AS t
    INNER JOIN bbls_with_multiple_zones AS bmz
        ON t.bbl = bmz.bbl
    INNER JOIN {{ source("build_sources", "transit_zones_atomic_geoms") }} AS td
        ON t.decomposed_id = td.decomposed_id
    INNER JOIN {{ source("build_sources", "pluto") }} AS p
        ON t.bbl = p.bbl
),

final AS (
    SELECT
        bbl,
        transit_zone,
        pct_covered,
        ambiguity_score,
        max_bbl_ambiguity_score,
        zone_decomposed_geometry,
        lot_geometry,
        lot_clipped_geometry
    FROM transit_zones_coverage_analysis
)

SELECT *
FROM final
ORDER BY max_bbl_ambiguity_score DESC, bbl ASC, pct_covered DESC
