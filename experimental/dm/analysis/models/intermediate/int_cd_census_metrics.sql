SELECT
    geoid::integer AS borocd,
    {{ census_counts() }}
FROM {{ ref('stg_pff__decennial') }}
WHERE geography = 'community_district'
GROUP BY geoid
