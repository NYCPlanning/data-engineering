WITH fire_companies_normalized AS (
    SELECT
        fc.globalid,
        fc.unit_short,
        fc.fire_division,
        fc.fire_battalion,
        fc.geom,
        -- Normalize unit_short by removing spaces
        REPLACE(fc.unit_short, ' ', '') AS unit_short_normalized,
        -- Extract fire company type (first character after removing spaces)
        LEFT(REPLACE(fc.unit_short, ' ', ''), 1) AS fire_company_type,
        -- Extract fire company number (everything after first character, after removing spaces)
        SUBSTRING(REPLACE(fc.unit_short, ' ', '') FROM 2) AS fire_company_number
    FROM {{ ref('stg__firecompany') }} AS fc
),
fire_companies_with_borough AS (
    SELECT
        fc.globalid,
        fc.unit_short,
        fc.fire_division,
        fc.fire_battalion,
        fc.fire_company_type,
        fc.fire_company_number,
        -- Handle special cases and determine borough
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
            -- patrol-borough joins - see docs/prod_bugs/002-police-geo-centroid-mismatch.md.
            ELSE (
                SELECT b.borocode
                FROM {{ ref('stg__borough') }} AS b
                WHERE ST_WITHIN(
                    CASE
                        WHEN ST_WITHIN(ST_CENTROID(fc.geom), fc.geom) THEN ST_CENTROID(fc.geom)
                        WHEN ST_POINTONSURFACE(fc.geom) IS NOT NULL THEN ST_POINTONSURFACE(fc.geom)
                        ELSE ST_CENTROID(fc.geom)
                    END,
                    b.geom
                )
            )
        END AS borough
    FROM fire_companies_normalized AS fc
)

SELECT
    globalid,
    unit_short,
    fire_company_type,
    fire_company_number,
    fire_division,
    fire_battalion,
    borough
FROM fire_companies_with_borough
WHERE borough IS NOT NULL  -- Only include fire companies with a valid borough
ORDER BY fire_company_type, fire_company_number
