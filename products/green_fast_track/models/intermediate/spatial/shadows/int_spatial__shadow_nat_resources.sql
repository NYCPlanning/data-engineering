WITH all_natural_resources AS (
{{ dbt_utils.union_relations(
    relations=[
        ref('int_spatial__natural_resources'),
        ref('int_flags__dob_natural_resources'),
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
    'shadow_nat_resources' AS flag_id_field_name,
    variable_type,
    variable_id,
    raw_geom,
    ST_Buffer(raw_geom, 200) AS buffer_geom
FROM all_natural_resources
