WITH historic_places_nyc AS (
    SELECT
        nrnum || ' - ' || historicname AS variable_id,
        ST_MAKEVALID(geom) AS geom
    FROM {{ ref('stg__nysparks_historicplaces') }}
    WHERE countyname IN ('Kings', 'New York', 'Queens', 'Richmond', 'Bronx')
),

grouped_historic_places AS (
    SELECT
        variable_id,
        ST_UNION(geom) AS raw_geom
    FROM historic_places_nyc
    GROUP BY variable_id
)

SELECT
    'us_historic_places' AS variable_type,
    variable_id,
    raw_geom,
    ST_BUFFER(raw_geom, 90) AS buffer
FROM grouped_historic_places
