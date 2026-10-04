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
        fcb.borough
    FROM fire_companies_normalized AS fc
    INNER JOIN {{ ref('int__fire_company_borough') }} AS fcb ON fc.globalid = fcb.globalid
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
