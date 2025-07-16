WITH all_natural_resources AS (
{{ dbt_utils.union_relations(
    relations=[
        ref('stg__nysdec_freshwater_wetlands'),
        ref('stg__nysdec_tidal_wetlands'),
        ref('stg__nysdec_priority_lakes'),
        ref('stg__nysdec_priority_estuaries'),
        ref('stg__nysdec_priority_streams'),
        ref('stg__dcp_beaches'),
        ref('stg__nysdec_natural_heritage_communities'),
        ref('stg__usfws_nyc_wetlands'),
        ref('stg__dcp_wrp_recognized_ecological_complexes'),
        ref('stg__dcp_wrp_special_natural_waterfront_areas'),
        ref('stg__dpr_forever_wild')
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
    'natural_resources' AS flag_id_field_name,
    variable_type,
    variable_id,
    st_multi(raw_geom) AS raw_geom
FROM all_natural_resources
