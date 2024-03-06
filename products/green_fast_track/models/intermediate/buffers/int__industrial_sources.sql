WITH pluto AS (
    SELECT
        bbl,
        landuse,
        geom
    FROM {{ ref('stg__pluto') }}
),

filtered AS (
    SELECT *
    FROM pluto
    WHERE landuse = '06'
)

SELECT
    'industrial_sources' AS variable_type,
    bbl AS variable_id,
    geom AS raw_geom,
    ST_BUFFER(geom, 400) AS buffer
FROM filtered
