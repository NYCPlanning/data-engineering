-- the stations in a community district or joint interest area, which together cover the city
SELECT stations.*
FROM {{ ref('stg_mta__rail_stations') }} AS stations
WHERE EXISTS (
    SELECT 1
    FROM {{ ref('int_cd_spine') }} AS district
    WHERE ST_Intersects(district.geom, stations.geom)
)
