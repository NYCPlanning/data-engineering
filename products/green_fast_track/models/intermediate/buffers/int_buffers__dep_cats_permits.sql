WITH cats_permits AS (
    SELECT
        variable_type,
        variable_id,
        permit_geom
    FROM
        {{ ref('stg__dep_cats_permits') }}
),

pluto AS (
    SELECT geom
    FROM {{ ref('stg__pluto') }}
),

cats_permits_with_pluto AS (
    SELECT
        s.variable_type,
        s.variable_id,
        COALESCE(p.geom, s.permit_geom) AS raw_geom
    FROM cats_permits AS s
    LEFT JOIN pluto AS p ON ST_WITHIN(s.permit_geom, p.geom)

),

-- create buffer of 400 feet around tax lot (bbl_geom column). 
-- If tax lot is null, create buffer around point (geom column)
final AS (
    SELECT
        variable_type,
        variable_id,
        raw_geom,
        ST_BUFFER(raw_geom, 400) AS buffer
    FROM cats_permits_with_pluto
)

SELECT * FROM final
