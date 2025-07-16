WITH title_v_facility_permits AS (
    SELECT
        variable_type,
        variable_id,
        permit_geom
    FROM
        {{ ref('stg__nysdec_title_v_facility_permits') }}
),

pluto AS (
    SELECT geom
    FROM {{ ref('stg__pluto') }}
),

title_v_facility_permits_with_pluto AS (
    SELECT
        t.variable_type,
        t.variable_id,
        t.permit_geom AS raw_geom,
        p.geom AS lot_geom
    FROM title_v_facility_permits AS t
    LEFT JOIN pluto AS p ON ST_Within(t.permit_geom, p.geom)

),

-- create buffer of 1,000 feet around tax lot (bbl_geom column). 
-- If tax lot is null, create buffer around point (geom column)
final AS (
    SELECT
        'title_v_permit' AS flag_id_field_name,
        variable_type,
        variable_id,
        ST_Multi(raw_geom) AS raw_geom,
        ST_Multi(lot_geom) AS lot_geom,
        ST_Multi(ST_Buffer(coalesce(lot_geom, raw_geom), 1000)) AS buffer_geom
    FROM title_v_facility_permits_with_pluto
)

SELECT * FROM final
