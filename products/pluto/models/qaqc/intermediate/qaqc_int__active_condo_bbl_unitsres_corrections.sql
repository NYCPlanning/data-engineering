{{ config(
    materialized = 'table'
) }}

WITH historical_condo_unit_corrections AS (
    SELECT
        bbl,
        old_value::numeric
    FROM {{ source("recipe_sources", "pluto_input_research") }}
    WHERE
        field = 'unitsres'
        AND substring(bbl, 7, 2) = '75'
),

primebbl_condo_units AS (
    SELECT
        primebbl,
        sum(coop_apts) AS coop_apts,
        sum(units) AS units
    FROM {{ source("build_sources", "pluto_rpad_geo") }}
    WHERE tl NOT LIKE '75%'
    GROUP BY primebbl
)

SELECT l.*
FROM historical_condo_unit_corrections AS l
INNER JOIN primebbl_condo_units AS r
    ON l.bbl = r.primebbl
WHERE l.old_value = r.coop_apts
