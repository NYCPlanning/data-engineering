WITH all_natural_resources AS (
{{ dbt_utils.union_relations(
    relations=[
        ref('stg__nyc_parks_properties'),
        ref('stg__dpr_schoolyard_to_playgrounds'),
        ref('int_spatial__pops'),
        ref('stg__waterfront_access_wpaa'),
        ref('stg__waterfront_access_pow'),
        ref('stg__nys_parks_properties'),
        ref('stg__us_parks_properties'),
    ],
    source_column_name="source_relation",
    include=["variable_type", "variable_id", "raw_geom", "lot_geom"],
    column_override={"raw_geom": "geometry", "lot_geom": "geometry"}
) }}
)
-- Note: without `column_override`, dbt throws an error trying to cast.
-- e.g.: `cast("raw_geom" as USER-DEFINED) as "raw...`

SELECT
    source_relation,
    'shadow_open_spaces' AS flag_id_field_name,
    variable_type,
    variable_id,
    ST_Multi(raw_geom) AS raw_geom,
    ST_Multi(lot_geom) AS lot_geom,
    ST_Multi(ST_Buffer(coalesce(lot_geom, raw_geom), 200)) AS buffer_geom
FROM all_natural_resources
