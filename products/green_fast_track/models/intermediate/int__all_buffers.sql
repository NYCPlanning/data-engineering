WITH all_buffers AS (
{{ dbt_utils.union_relations(
    relations=[
        ref('int__elevated_railways'),
        ref('int__nysdec_state_facility_permits'),
        ref('int__nysdec_title_v_facility_permits'),
        ref('int__dep_cats_permits'),
        ref('int__industrial_sources'),
        ref('stg__dcm_arterial_highways'),
        ref('stg__dcp_air_quality_vent_towers_buffered'),
        ref('stg__panynj_airports')
    ],
    column_override={"raw_geom": "geometry", "buffer": "geometry"}
) }}
)
-- Note: without `column_override`, dbt throws an error trying to cast.
-- e.g.: `cast("raw_geom" as USER-DEFINED) as "raw...`

SELECT
    variable_type,
    variable_id,
    raw_geom,
    buffer
FROM all_buffers
