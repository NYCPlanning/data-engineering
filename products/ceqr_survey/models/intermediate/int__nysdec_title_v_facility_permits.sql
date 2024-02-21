-- int__nysdec_title_v_facility_permits.sql

WITH title_v_facility_permits AS (
    SELECT
        variable,
        permit_id AS id,
        geom AS permit_geom
    FROM
        {{ ref('stg__nysdec_title_v_facility_permits') }}
),

pluto AS (
    SELECT geom
    FROM {{ ref('stg__pluto') }}
),

title_v_facility_permits_with_pluto AS (
    SELECT
        t.variable,
        t.id,
        t.permit_geom,
        p.geom AS bbl_geom

    FROM title_v_facility_permits AS t
    LEFT JOIN pluto AS p ON ST_WITHIN(t.permit_geom, p.geom)

),

-- create buffer of 1,000 feet around tax lot (bbl_geom column). 
-- If tax lot is null, create buffer around point (geom column)
final AS (
    SELECT
        variable,
        id,
        ST_BUFFER(COALESCE(bbl_geom, permit_geom), 1000) AS geom
    FROM title_v_facility_permits_with_pluto
)

SELECT * FROM final
