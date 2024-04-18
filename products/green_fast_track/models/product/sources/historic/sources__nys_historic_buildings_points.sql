SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref('stg__nysshpo_historic_buildings') }}
