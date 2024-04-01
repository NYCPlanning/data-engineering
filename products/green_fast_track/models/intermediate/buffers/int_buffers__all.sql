WITH all_buffers AS (
{{ dbt_utils.union_relations(
    relations=[
        ref('int_buffers__dcm_arterial_highways'),
        ref('int_buffers__dcp_air_quality_vent_towers'),
        ref('int_buffers__dep_cats_permits'),
        ref('int_buffers__elevated_railways'),
        ref('int_buffers__industrial_sources'),
        ref('int_buffers__nyc_parks_properties'),
        ref('int_buffers__nys_parks_properties'),
        ref('int_buffers__nysdec_state_facility_permits'),
        ref('int_buffers__nysdec_title_v_facility_permits'),
        ref('int_buffers__pops'),
        ref('int_buffers__us_parks_properties'),
        ref('int_buffers__waterfront_access_pow'),
        ref('int_buffers__waterfront_access_wpaa'),
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
