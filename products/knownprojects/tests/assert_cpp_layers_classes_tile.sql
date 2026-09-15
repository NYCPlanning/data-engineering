-- The five break classes must tile the positive range with no gap and no
-- overlap: class 1 starts at 0 and each later class starts where the previous
-- one ended. A gap here means some value falls between two classes.

WITH ordered AS (
    SELECT
        geo,
        "range",
        "rangeMin",
        "rangeMax",
        row_number() OVER (PARTITION BY geo, "range" ORDER BY "rangeMin") AS class_id,
        lag("rangeMax") OVER (PARTITION BY geo, "range" ORDER BY "rangeMin") AS prev_max
    FROM {{ ref('cpp_housing_growth_layers') }}
    -- boroughs aren't classified, so they don't tile
    WHERE "rangeLabel" != 'Net loss' AND geo != 'boro'
)

SELECT geo, "range", class_id, "rangeMin", "rangeMax", prev_max
FROM ordered
WHERE
    (class_id = 1 AND "rangeMin" != 0)
    OR (class_id > 1 AND "rangeMin" != prev_max)
    OR "rangeMax" < "rangeMin"
