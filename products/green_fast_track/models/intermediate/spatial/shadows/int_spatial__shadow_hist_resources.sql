WITH all_natural_resources AS (
{{ dbt_utils.union_relations(
    relations=[
        ref('stg__lpc_landmarks'),
        ref('stg__nysshpo_historic_buildings'),
        ref('stg__lpc_scenic_landmarks'),
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
    variable_type,
    variable_id,
    raw_geom,
    ST_BUFFER(raw_geom, 200) AS buffer_geom
FROM all_natural_resources
