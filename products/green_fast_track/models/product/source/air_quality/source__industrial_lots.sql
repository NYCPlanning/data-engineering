SELECT
    variable_id,
    lot_geom
FROM {{ ref('int_spatial__industrial_lots') }}
