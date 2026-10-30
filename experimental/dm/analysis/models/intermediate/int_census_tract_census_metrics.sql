SELECT
    geoid AS boroct2020,
    {{ census_counts() }}
FROM {{ ref('stg_pff__decennial') }}
WHERE geography = 'census_tract'
GROUP BY geoid
