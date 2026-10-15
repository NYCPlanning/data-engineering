{{ config(materialized='table', tags=['cpp']) }}

-- Housing growth by borough, for the Capital Projects Portal.
--
-- Rolled up from the CD model so each borough equals the sum of its CDs. An NTA
-- rollup by borocode would not: Rikers Island is in Bronx County but Queens CD 1.

{%- set latest = cpp_latest_year() | int -%}
{%- set past_col = 'completed_units_' ~ (latest - 9) ~ '_' ~ latest -%}
{%- set proj_col = 'projected_completed_units_' ~ (latest + 1) ~ '_' ~ (latest + 10) %}

WITH boroughs AS (
    SELECT
        borocode::text AS geography_id,
        boroname AS borough_name
    FROM {{ source('recipe_sources', 'dcp_boroboundaries') }}
),

rolled_up AS (
    SELECT
        left(a.geography_id, 1) AS geography_id,
        sum(a.units_2020_census) AS units_2020_census,
        sum(a.units_2020) AS units_2020,
        sum(a.{{ past_col }}) AS {{ past_col }},
        sum(a.completed_units_2021_{{ latest }}) AS completed_units_2021_{{ latest }},
        sum(a.units_{{ latest }}) AS units_{{ latest }},
        sum(a.{{ proj_col }}) AS {{ proj_col }},
        sum(a.projected_units_{{ latest + 10 }}) AS projected_units_{{ latest + 10 }}
    FROM {{ ref('cpp_housing_growth_cd') }} AS a
    GROUP BY left(a.geography_id, 1)
)

SELECT
    a.geography_id,
    b.borough_name,
    a.units_2020_census,
    a.units_2020,
    a.{{ past_col }},
    a.completed_units_2021_{{ latest }},
    a.units_{{ latest }},
    a.{{ proj_col }},
    a.projected_units_{{ latest + 10 }}
FROM rolled_up AS a
INNER JOIN boroughs AS b ON a.geography_id = b.geography_id
ORDER BY a.geography_id ASC
