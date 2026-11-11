SELECT
    blockid20 AS census_block_2020,
    acre_60 AS acres_reachable_in_60_minutes,
    -- acre_60_pct, cut to 10 characters by the shapefile
    acre_60_pc AS acres_reachable_percentile,
    category AS access_category,
    -- the category's lower percentile, such as 70 for 70th–75th percentile, to sort the categories by
    regexp_extract(category, '^(\d+)', 1)::INTEGER AS access_category_floor,
    geom
FROM {{ source('sources', 'transit_land_access') }}
