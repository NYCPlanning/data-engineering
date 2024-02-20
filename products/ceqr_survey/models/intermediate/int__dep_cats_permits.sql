-- int__dep_cats_permits.sql

WITH cats_permits AS (
    SELECT
        variable,
        applicationid as id,
        geom as permit_geom
    FROM
        {{ ref('stg__dep_cats_permits') }}
),

pluto AS (
    SELECT geom
    FROM {{ ref('stg__pluto') }}
),

cats_permits_with_pluto AS (
    SELECT
        s.variable,
        s.id,
        s.permit_geom,
        p.geom AS bbl_geom

    FROM cats_permits AS s
    LEFT JOIN pluto AS p ON ST_WITHIN(s.permit_geom, p.geom)

),

-- create buffer of 400 feet around tax lot (bbl_geom column). 
-- If tax lot is null, create buffer around point (geom column)
final AS (
    SELECT
        variable,
        id,
        (CASE
            WHEN bbl_geom IS NULL THEN ST_BUFFER(permit_geom, 400)
            ELSE ST_BUFFER(bbl_geom, 400)
        END) AS geom
    FROM cats_permits_with_pluto
)

SELECT * FROM final
