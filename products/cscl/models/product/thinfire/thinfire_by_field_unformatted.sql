WITH fire_companies_normalized AS (
    SELECT
        fc.globalid,
        fc.unit_short,
        fc.fire_division,
        fc.fire_battalion,
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
            -- For all other cases, find borough from any matching Atomic Polygon
            ELSE (
                SELECT ap.borocode
                FROM {{ ref('stg__atomicpolygons') }} AS ap
                WHERE ap.fire_company_type || ap.fire_company_number = fc.unit_short_normalized
                ORDER BY ap.atomicid
                LIMIT 1
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
