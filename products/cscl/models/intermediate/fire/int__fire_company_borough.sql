{{ config(
    materialized='table'
) }}

-- Borough of each fire company: the company polygon's centroid (falling back to a point
-- on its surface) against the topology-built borough boundary, with the ETL spec's
-- documented exceptions. The one geometric step in THINFIRE, computed here so the
-- product model is plain joins.

WITH fire_companies_normalized AS (
    SELECT
        fc.globalid,
        fc.geom,
        replace(fc.unit_short, ' ', '') AS unit_short_normalized
    FROM {{ ref('stg__firecompany') }} AS fc
)

SELECT
    fc.globalid,
    CASE
        -- Special case: E-81 belongs to Bronx even though it covers Marble Hill (Manhattan)
        WHEN fc.unit_short_normalized = 'E81' THEN '2'
        -- Special case: E-260 belongs to Queens even though it covers Roosevelt Island (Manhattan)
        WHEN fc.unit_short_normalized = 'E260' THEN '4'
        -- Special case: E-263 belongs to Queens even though it covers Rikers Island (Bronx)
        WHEN fc.unit_short_normalized = 'E263' THEN '4'
        -- For all other cases: the ETL spec (Table 25, Note 1) documents the "correct"
        -- method as a company-centroid point-in-polygon match against boroughs, but
        -- recommends an AtomicPolygon-attribute-match shortcut instead "for performance
        -- efficiency" - ESRI-desktop-era guidance that doesn't apply here. That shortcut
        -- (picking an arbitrary matching AtomicPolygon - spec's own words: "any of the
        -- matching AP's") disagreed with prod for every company whose coverage spans a
        -- borough boundary, since prod's arbitrary pick and ours have no reason to agree.
        -- Verified this centroid method against all 12 known-mismatched companies plus
        -- the 3 special cases above (which it resolves correctly without needing them,
        -- kept anyway as an explicit backstop matching the spec's documented exceptions)
        -- and the full company roster: exactly those 12 change, zero regressions
        -- elsewhere. Same centroid/fallback pattern as the police precinct/sector/
        -- patrol-borough joins - see Bug 002 in data_issues.md.
        -- Boroughs come from the AP topology's water-included boundary (bbwi), not the
        -- land-only one (bb): bbwi covers the same area as the source borough polygons,
        -- so a centroid falling in water still lands in a borough. Same result as the
        -- source polygons for all 348 companies, the 3 special cases included.
        ELSE (
            SELECT b.entity_id::text
            FROM {{ ref('int__boundary__bbwi') }} AS b
            WHERE st_within(
                CASE
                    WHEN st_within(st_centroid(fc.geom), fc.geom) THEN st_centroid(fc.geom)
                    WHEN st_pointonsurface(fc.geom) IS NOT NULL THEN st_pointonsurface(fc.geom)
                    ELSE st_centroid(fc.geom)
                END,
                b.geom
            )
        )
    END AS borough
FROM fire_companies_normalized AS fc
