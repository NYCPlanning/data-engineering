WITH pluto AS (
    SELECT
        geom,
        bbl
    FROM {{ ref('stg__pluto') }}
),

historic_places_nyc AS (
    SELECT
        national_register_num,
        resource_name,
        pluto.bbl AS pluto_bbl,
        pluto.geom AS pluto_geom,
        COALESCE(pluto.geom, hpnyc.geom) AS raw_geom
    FROM {{ ref('stg__nysshpo_register_historic_places') }} AS hpnyc
    LEFT JOIN pluto ON ST_INTERSECTS(pluto.geom, hpnyc.geom)
    WHERE county IN ('Kings', 'New York', 'Queens', 'Richmond', 'Bronx')
)

SELECT
    pluto_geom,
    'state_historic_place' AS variable_type,
    CONCAT(national_register_num, ' - ', resource_name) AS viariable_id,
    raw_geom,
    ST_BUFFER(raw_geom, 90) AS buffer
FROM historic_places_nyc
