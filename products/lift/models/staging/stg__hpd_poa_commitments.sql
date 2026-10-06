-- HPD's Points of Agreement (POA) commitments tracker (2026-09-29 extract): one row
-- per city-owned site x bbl with a POA/community commitment tied to a prior
-- rezoning or disposition. Replaces the old NYC Rezoning Tracker match (spatial, via
-- rezoning_area - see git history) - this source already carries bbl directly, so
-- lift_supplemented joins straight to it, no spatial match needed.
-- The DuckDB loader doesn't sanitize CSV headers (unlike the postgres-backed path),
-- so multi-word source columns need a quoted identifier (`"City Agencies
-- Jurisdiction"`, `"POA/Community"`).
-- At least one bbl is known-bad: 400024007 is only 9 digits (likely a dropped leading
-- zero on the lot number somewhere upstream) and won't match any LIFT bbl. Left as-is
-- rather than guessed at - see README Limitations.
WITH raw AS (
    SELECT * FROM {{ source('recipe_sources', 'hpd_poa_commitments') }}
),

final AS (
    SELECT
        TRY_CAST(bbl AS BIGINT) AS bbl,
        site,
        "City Agencies Jurisdiction" AS city_agency_jurisdiction,
        borough,
        cm AS council_member,
        cd AS community_district,
        neighborhood,
        "POA/Community" AS poa_community
    FROM raw
    WHERE TRY_CAST(bbl AS BIGINT) IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_CAST(bbl AS BIGINT) ORDER BY ogc_fid) = 1
)

SELECT * FROM final
