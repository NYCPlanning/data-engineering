-- int__nysdec_state_facility_permits.sql

WITH state_facility_permits AS (
    SELECT
        variable,
        permit_id AS id,
        geom AS permit_geom
    FROM
        {{ ref('stg__nysdec_state_facility_permits') }}
),

pluto AS (
    SELECT geom
    FROM {{ ref('stg__pluto') }}
),

state_facility_permits_with_pluto AS (
    SELECT
        s.variable,
        s.id,
        s.permit_geom,
        p.geom AS bbl_geom

    FROM state_facility_permits AS s
    LEFT JOIN pluto AS p ON ST_WITHIN(s.permit_geom, p.geom)

),

-- create buffer of 1,000 feet around tax lot (bbl_geom column). 
-- If tax lot is null, create buffer around point (geom column)
final AS (
    SELECT
        variable,
        id,
        (CASE
            WHEN bbl_geom IS NULL THEN ST_BUFFER(permit_geom, 1000)
            ELSE ST_BUFFER(bbl_geom, 1000)
        END) AS geom
    FROM state_facility_permits_with_pluto
)

SELECT * FROM final
