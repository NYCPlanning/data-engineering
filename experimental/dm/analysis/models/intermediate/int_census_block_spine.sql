-- one row per 2020 block, in the district holding most of its area. That reproduces Population FactFinder's own
-- district totals for 55 of 71 districts, and the rest within 1,105 people. FactFinder doesn't publish its block
-- to district assignment.
SELECT
    block.census_block_2020,
    block.bctcb2020,
    coalesce(population.population_2020, 0) AS population_2020,
    arg_max(district.borocd, ST_Area(ST_Intersection(district.geom, block.geom))) AS borocd
FROM {{ ref('stg_dcp__cb2020') }} AS block
INNER JOIN {{ ref('int_cd_spine') }} AS district ON ST_Intersects(district.geom, block.geom)
LEFT JOIN {{ ref('stg_pff__block_population') }} AS population ON block.bctcb2020 = population.bctcb2020
GROUP BY block.census_block_2020, block.bctcb2020, population.population_2020
