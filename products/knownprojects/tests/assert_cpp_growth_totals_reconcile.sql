-- NTAs, community districts and boroughs all tile the city, so their citywide
-- totals must agree. Catches a join that matched only part of one geography's
-- keys. This is citywide only on purpose: per borough, CD and NTA disagree by
-- 878 projected units until issue #2633 is fixed.
{% set latest = var('cpp_latest_complete_year') | int %}
WITH nta AS (
    SELECT
        sum(units_2020) AS u2020,
        sum(units_{{ latest }}) AS u_latest
    FROM {{ ref('cpp_housing_growth_nta') }}
),

cd AS (
    SELECT
        sum(units_2020) AS u2020,
        sum(units_{{ latest }}) AS u_latest
    FROM {{ ref('cpp_housing_growth_cd') }}
),

boro AS (
    SELECT
        sum(units_2020) AS u2020,
        sum(units_{{ latest }}) AS u_latest
    FROM {{ ref('cpp_housing_growth_boro') }}
)

SELECT
    nta.u2020 AS nta_units_2020,
    cd.u2020 AS cd_units_2020,
    boro.u2020 AS boro_units_2020,
    nta.u_latest AS nta_units_{{ latest }},
    cd.u_latest AS cd_units_{{ latest }},
    boro.u_latest AS boro_units_{{ latest }}
FROM nta, cd, boro
WHERE
    nta.u2020 != cd.u2020
    OR nta.u2020 != boro.u2020
    OR nta.u_latest != cd.u_latest
    OR nta.u_latest != boro.u_latest
