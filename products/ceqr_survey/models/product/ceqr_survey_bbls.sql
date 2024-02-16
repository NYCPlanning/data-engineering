WITH bbls AS (
    SELECT * FROM
        {{ ref('stg__pluto') }}
),

ceqr_survery_bbls AS (
    SELECT
        bbl,
        0 AS zoning_districts_check,
        null AS zoning_districts_values,
        0 AS zoning_coastal_risk_districts_check,
        null AS zoning_coastal_risk_districts_values
    FROM bbls
)

SELECT * FROM ceqr_survery_bbls
