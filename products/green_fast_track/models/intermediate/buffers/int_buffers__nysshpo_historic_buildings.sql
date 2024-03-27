WITH pluto AS (
    SELECT
        geom,
        bbl
    FROM {{ ref('stg__pluto') }}
),

historic_buildings_nyc AS (
    {{ clip_to_geom(ref('stg__nysshpo_historic_buildings')) }} -- noqa
),

historic_buildings_nyc_filtered AS (
    SELECT
        hbnyc.*,
        pluto.geom AS pluto_geom,
        pluto.bbl AS pluto_bbl
    FROM historic_buildings_nyc AS hbnyc
    LEFT JOIN pluto ON ST_INTERSECTS(pluto.geom, hbnyc.geom)
    WHERE hbnyc.geom IS NOT NULL
)

SELECT
    usnnum AS variable_id,
    'historic_buildings' AS variable_type,
    COALESCE(pluto_geom, geom) AS raw_geom,
    ST_BUFFER(COALESCE(pluto_geom, geom), 90) AS buffer
FROM historic_buildings_nyc_filtered
WHERE eligibilitydesc IN ('Eligible', 'Listed')
