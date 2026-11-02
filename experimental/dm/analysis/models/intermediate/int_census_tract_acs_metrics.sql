SELECT
    geoid AS boroct2020,
    {{ household_income_metrics() }}
FROM {{ ref('stg_pff__acs') }}
WHERE geography = 'census_tract'
GROUP BY geoid
