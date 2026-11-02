SELECT
    geoid AS nta2020,
    {{ household_income_metrics() }}
FROM {{ ref('stg_pff__acs') }}
WHERE geography = 'nta'
GROUP BY geoid
