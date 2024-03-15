WITH all_natural_resources AS (
{{ dbt_utils.union_relations(
    relations=[
        ref('stg__dcp_beaches'),
        ref('stg__nysdec_freshwater_wetlands'),
        ref('stg__nysdec_natural_heritage_communities'),
        ref('stg__nysdec_priority_estuaries'),
        ref('stg__nysdec_priority_lakes'),
        ref('stg__nysdec_priority_shorelines'),
        ref('stg__nysdec_priority_streams'),
        ref('stg__nysdec_tidal_wetlands'),
        ref('stg__usfws_nyc_wetlands'),
    ],
    column_override={"raw_geom": "geometry", "buffer": "geometry"}
) }}
)
-- Note: without `column_override`, dbt throws an error trying to cast.
-- e.g.: `cast("raw_geom" as USER-DEFINED) as "raw...`

SELECT
    'natural_resource_shadow' AS variable_type,
    'Natural Resource Shadow' AS variable_id,
    st_union(raw_geom) AS raw_geom,
    st_buffer(st_union(raw_geom), 200) AS buffer
FROM all_natural_resources
