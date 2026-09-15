{{ config(materialized='table', tags=['cpp']) }}

-- Housing growth by borough, for the Capital Projects Portal.
--
-- Rolled up from the NTA model rather than allocated afresh.
--
-- NTA and not CD, because CD is allocated against water-included boundaries
-- whose Manhattan polygons reach the middle of the East River. Eleven Brooklyn
-- waterfront projects are split across the river into a Manhattan CD, so a CD
-- rollup credits Manhattan with units that belong to Brooklyn (878 projected
-- units on the 26Q2 build). NTA boundaries are land only and split none of
-- them. See issue #2633.
--
-- Until that is fixed this model will not agree with cpp_housing_growth_cd per
-- borough on projected, though citywide totals tie.

{%- set latest = cpp_latest_year() | int -%}
{%- set past_col = 'completed_units_' ~ (latest - 9) ~ '_' ~ latest -%}
{%- set proj_col = 'projected_completed_units_' ~ (latest + 1) ~ '_' ~ (latest + 10) %}

WITH boroughs AS (
    SELECT
        borocode::text AS geography_id,
        boroname AS borough_name
    FROM {{ source('recipe_sources', 'dcp_boroboundaries') }}
),

nta_borough AS (
    SELECT
        nta2020,
        borocode::text AS geography_id
    FROM {{ source('recipe_sources', 'dcp_nta2020') }}
),

rolled_up AS (
    SELECT
        b.geography_id,
        sum(a.units_2020_census) AS units_2020_census,
        sum(a.units_2020) AS units_2020,
        sum(a.{{ past_col }}) AS {{ past_col }},
        sum(a.completed_units_2021_{{ latest }}) AS completed_units_2021_{{ latest }},
        sum(a.units_{{ latest }}) AS units_{{ latest }},
        sum(a.{{ proj_col }}) AS {{ proj_col }},
        sum(a.projected_units_{{ latest + 10 }}) AS projected_units_{{ latest + 10 }}
    FROM {{ ref('cpp_housing_growth_nta') }} AS a
    INNER JOIN nta_borough AS b ON a.geography_id = b.nta2020
    GROUP BY b.geography_id
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
