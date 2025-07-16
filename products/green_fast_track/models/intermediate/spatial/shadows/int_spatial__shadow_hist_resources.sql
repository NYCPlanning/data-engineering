SELECT
    source_relation,
    'shadow_hist_resources' AS flag_id_field_name,
    variable_type,
    variable_id,
    raw_geom,
    lot_geom,
    st_multi(st_buffer(coalesce(lot_geom, raw_geom), 200)) AS buffer_geom
FROM {{ ref('int_spatial__historic_resources') }}
