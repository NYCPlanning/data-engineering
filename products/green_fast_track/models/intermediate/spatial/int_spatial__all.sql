{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['buffer_geom'], 'type': 'gist'},
    ]
) }}

WITH all_buffers AS (
{{ dbt_utils.union_relations(
    relations=[
        ref('int_spatial__cats_permits'),
        ref('int_spatial__industrial_lots'),
        ref('int_spatial__state_facility'),
        ref('int_spatial__title_v_permit'),
        ref('int_spatial__vent_tower'),
        ref('int_spatial__arterial_highway'),

        ref('stg__panynj_airports'),
        ref('int_spatial__exposed_railway'),

        ref('int_spatial__natural_resources'),
        ref('stg__nysdec_freshwater_wetlands_checkzones'),

        ref('stg__nysshpo_archaeological_buffer_areas'),
        ref('int_spatial__historic_districts'),
        ref('int_spatial__historic_resources'),
        ref('int_spatial__historic_resources_adj'),

        ref('int_spatial__shadow_open_spaces'),
        ref('int_spatial__shadow_nat_resources'),
        ref('int_spatial__shadow_hist_resources'),

    ],
    source_column_name="source_relation",
    include=["flag_id_field_name", "variable_type", "variable_id", "raw_geom", "lot_geom", "buffer_geom"],
    column_override={"raw_geom": "geometry", "lot_geom": "geometry", "buffer_geom": "geometry"}
) }}
)
-- Note: without `column_override`, dbt throws an error trying to cast.
-- e.g.: `cast("raw_geom" as USER-DEFINED) as "raw...`

SELECT
    source_relation,
    flag_id_field_name,
    variable_type,
    variable_id,
    ST_Multi(raw_geom) AS raw_geom,
    ST_Multi(lot_geom) AS lot_geom,
    ST_Multi(buffer_geom) AS buffer_geom,
    ST_Multi(coalesce(buffer_geom, lot_geom, raw_geom)) AS variable_geom
FROM all_buffers
