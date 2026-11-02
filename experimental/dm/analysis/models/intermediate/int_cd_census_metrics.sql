SELECT
    -- try_cast because DuckDB can cast before the geography filter, and NTA geoids aren't numeric
    try_cast(geoid AS integer) AS borocd,
    {{ census_counts() }}
FROM {{ ref('stg_pff__decennial') }}
WHERE geography = 'community_district'
GROUP BY geoid
