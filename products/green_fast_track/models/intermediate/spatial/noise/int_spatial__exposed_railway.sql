WITH all_rails AS (
{{ dbt_utils.union_relations(
    relations=[
        ref('stg__exposed_railways'),
        ref('stg__exposed_railyards')
    ],
    source_column_name="source_relation",
    include=["variable_type", "variable_id", "raw_geom"],
    column_override={"raw_geom": "geometry"}
) }}
)
-- Note: without `column_override`, dbt throws an error trying to cast.
-- e.g.: `cast("raw_geom" as USER-DEFINED) as "raw...`

SELECT
    'exposed_railway' AS flag_id_field_name,
    variable_type,
    variable_id,
    raw_geom,
    ST_Buffer(raw_geom, 1500) AS buffer_geom
FROM all_rails
