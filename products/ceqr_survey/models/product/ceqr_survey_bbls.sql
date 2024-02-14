WITH bbls AS (
    SELECT * FROM
        {{ ref('stg__pluto') }}
),

ceqr_survery_bbls AS (
    SELECT
        bbl,
        null AS variable_name,
        true AS variable_outcome,
        null AS variable_value
    FROM bbls
)

SELECT * FROM ceqr_survery_bbls
