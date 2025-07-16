WITH all_natural_resources AS (
{{ dbt_utils.union_relations(
    relations=[
        ref('stg__lpc_historic_district_areas'),
        ref('stg__nysshpo_historic_building_districts'),
        ref('stg__lpc_scenic_landmarks'),
        ref('stg__nysparks_historicplaces')
    ],
    source_column_name="source_relation",
    include=["variable_type", "variable_id", "raw_geom"],
    column_override={"raw_geom": "geometry"}
) }}
)
-- Note: without `column_override`, dbt throws an error trying to cast.
-- e.g.: `cast("raw_geom" as USER-DEFINED) as "raw...`

SELECT
    source_relation,
    'historic_districts' AS flag_id_field_name,
    variable_type,
    variable_id,
    ST_Multi(raw_geom) AS raw_geom
FROM all_natural_resources
