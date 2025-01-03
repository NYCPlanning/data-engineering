SELECT
    geoid20 AS bctcb2020,
    borocode::integer AS borough_code,
    geogname AS borough_name,
    "pop1.1"::numeric AS total_population   -- noqa: RF01
FROM {{ source("recipe_sources", "dcp_censusdata_blocks") }}
WHERE geogtype = 'CB2020'
