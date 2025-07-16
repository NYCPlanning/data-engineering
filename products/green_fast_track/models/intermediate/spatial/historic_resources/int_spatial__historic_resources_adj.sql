SELECT
    source_relation,
    'historic_resources_adj' AS flag_id_field_name,
    variable_type,
    variable_id,
    lot_geom,
    raw_geom,
    st_multi(st_buffer(coalesce(lot_geom, raw_geom), 90)) AS buffer_geom
FROM {{ ref('int_spatial__historic_resources') }}
