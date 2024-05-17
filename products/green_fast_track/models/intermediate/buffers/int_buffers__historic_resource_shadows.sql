WITH non_dist_historic AS (
{{ dbt_utils.union_relations(
    relations=[
        ref('int_buffers__lpc_landmarks'),
        ref('int_buffers__nysparks_historicplaces'),
        ref('stg__lpc_historic_district_areas'),
        ref('stg__lpc_scenic_landmarks'),
        ref('stg__nysshpo_archaeological_buffer_areas'),
        ref('stg__nysshpo_historic_buildings'),
    ],
    column_override={"raw_geom": "geometry", "buffer": "geometry"},
    include=["raw_geom"]
) }}
)
-- Note: without `column_override`, dbt throws an error trying to cast.
-- e.g.: `cast("raw_geom" as USER-DEFINED) as "raw...`


SELECT
    'historic_resource_shadow' AS variable_type,
    'Historic Resource Shadow' AS variable_id,
    raw_geom,
    st_buffer(raw_geom, 200) AS buffer
FROM non_dist_historic
