{{ config(
    materialized = 'view'
) }}
-- Analysis: MIH Areas Questionable Assignments
-- Purpose: Identify tax lots with multiple MIH area assignments and analyze coverage patterns
--
-- This analysis identifies BBLs that intersect with multiple MIH areas
-- and pulls in the relevant geometries for analysis purposes.

WITH bbls_with_multiple_mihareas AS (
    -- Find BBLs that have multiple MIH area assignments
    SELECT bbl
    FROM {{ source("build_sources", "mihperorder") }}
    WHERE row_number = 2  -- BBLs with at least 2 MIH assignments
),

mihareas_coverage_analysis AS (
    -- Calculate coverage metrics for BBLs with multiple MIH area assignments
    SELECT
        m.bbl,
        m.project_name,
        m.affordability_option,
        m.perbblgeom AS pct_covered,
        p.geom AS lot_geometry,
        p.address,
        d.wkb_geometry AS mih_geometry,
        -- Calculate how close the coverage percentage is to 50% (most ambiguous case)
        50 - ABS(m.perbblgeom - 50.0) AS ambiguity_score,
        -- Get the maximum ambiguity score for each BBL to identify most questionable cases
        MAX(50 - ABS(m.perbblgeom - 50.0)) OVER (PARTITION BY m.bbl) AS max_bbl_ambiguity_score
    FROM {{ source("build_sources", "mihperorder") }} AS m
    INNER JOIN bbls_with_multiple_mihareas AS bmm
        ON m.bbl = bmm.bbl
    INNER JOIN {{ source("build_sources", "pluto") }} AS p
        ON m.bbl = p.bbl
    INNER JOIN {{ source("build_sources", "dcp_mih") }} AS d
        ON
            m.project_name = d.project_name
            AND m.affordability_option = d.mih_option
),

final AS (
    SELECT DISTINCT ON (project_name, affordability_option, bbl, max_bbl_ambiguity_score)
        bbl,
        project_name,
        affordability_option,
        pct_covered,
        ambiguity_score,
        max_bbl_ambiguity_score,
        mih_geometry,
        lot_geometry,
        address
    FROM mihareas_coverage_analysis
)

SELECT *
FROM final
ORDER BY max_bbl_ambiguity_score DESC, bbl ASC, pct_covered DESC
