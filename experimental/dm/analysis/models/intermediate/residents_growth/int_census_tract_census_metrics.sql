SELECT
    geoid AS boroct2020,
    {{ age_group_counts() }},
    {{ census_counts() }}
FROM {{ ref('stg_pff__decennial') }}
WHERE geography = 'census_tract'
GROUP BY geoid
