-- 2020 block population, the only block rows used. The file has dozens of variables for each block, so blocks are
-- kept out of stg_pff__decennial. Block geoids are bctcb2020: borough, tract, and block.
SELECT
    geoid AS bctcb2020,
    value AS population_2020
FROM {{ source('sources', 'pff_decennial_2020') }}
WHERE variable = 'pop1' AND length(geoid) = 11 AND regexp_matches(geoid, '^[0-9]+$')
