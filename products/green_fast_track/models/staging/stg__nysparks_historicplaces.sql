WITH historic_places AS (
    SELECT * FROM {{ source('recipe_sources', 'nysparks_historicplaces') }}
),

all_historic_places AS (
    SELECT
        nrnum,
        historicname,
        countyname,
        citytown,
        st_transform(wkb_geometry, 2263) AS geom
    FROM historic_places
),

historic_places_nyc AS (
    SELECT
        nrnum || ' - ' || historicname AS variable_id,
        st_makevalid(geom) AS geom
    FROM all_historic_places
    WHERE countyname IN ('Kings', 'New York', 'Queens', 'Richmond', 'Bronx')
    -- citytown overall is a messier field, but there's a specific troublesome row to be filtered out.
    AND citytown <> 'Buffalo'
)

SELECT
    'us_historic_places' AS variable_type,
    variable_id,
    st_union(geom) AS raw_geom
FROM historic_places_nyc
GROUP BY variable_id
