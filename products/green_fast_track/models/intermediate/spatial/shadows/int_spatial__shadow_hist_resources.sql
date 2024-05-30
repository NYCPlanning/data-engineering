WITH all_shadow_resources AS (
{{ dbt_utils.union_relations(
    relations=[
        ref('int_spatial__historic_resources'),
        ref('stg__lpc_scenic_landmarks'),
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
    'shadow_hist_resources' AS flag_id_field_name,
    variable_type,
    variable_id,
    ST_MULTI(raw_geom) AS raw_geom,
    ST_MULTI(lot_geom) AS lot_geom,
    ST_MULTI(ST_BUFFER(COALESCE(lot_geom, raw_geom), 200)) AS buffer_geom
FROM all_shadow_resources
