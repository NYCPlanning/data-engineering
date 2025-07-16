WITH state_facility_permits AS (
    SELECT
        variable_type,
        variable_id,
        permit_geom
    FROM
        {{ ref('stg__nysdec_state_facility_permits') }}
),

pluto AS (
    SELECT geom
    FROM {{ ref('stg__pluto') }}
),

state_facility_permits_with_pluto AS (
    SELECT
        s.variable_type,
        s.variable_id,
        s.permit_geom AS raw_geom,
        p.geom AS lot_geom
    FROM state_facility_permits AS s
    LEFT JOIN pluto AS p ON st_within(s.permit_geom, p.geom)

),

-- create buffer of 1,000 feet around tax lot (bbl_geom column). 
-- If tax lot is null, create buffer around point (geom column)
final AS (
    SELECT
        'state_facility' AS flag_id_field_name,
        variable_type,
        variable_id,
        st_multi(raw_geom) AS raw_geom,
        st_multi(lot_geom) AS lot_geom,
        st_multi(st_buffer(coalesce(lot_geom, raw_geom), 1000)) AS buffer_geom
    FROM state_facility_permits_with_pluto
)

SELECT * FROM final
