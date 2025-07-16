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
    'industrial_lots' AS flag_id_field_name,
    'industrial_sources' AS variable_type,
    bbl AS variable_id,
    geom AS raw_geom,
    geom AS lot_geom,
    st_buffer(geom, 400) AS buffer_geom
FROM filtered
