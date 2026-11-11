SELECT
    geoid AS nta2020,
    {{ age_group_counts() }},
    {{ census_counts() }}
FROM {{ ref('stg_pff__decennial') }}
WHERE geography = 'nta'
GROUP BY geoid
