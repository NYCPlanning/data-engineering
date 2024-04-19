-- natural resources may still need buffers added
{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['buffer'], 'type': 'gist'},
    ]
) }}


WITH all_buffers AS (
{{ dbt_utils.union_relations(
    relations=[
        ref('int_buffers__dcm_arterial_highways'),
        ref('int_buffers__dcp_air_quality_vent_towers'),
        ref('int_buffers__dep_cats_permits'),
        ref('int_buffers__exposed_railways'),
        ref('int_buffers__industrial_sources'),
        ref('int_buffers__nyc_parks_properties'),
        ref('int_buffers__dpr_schoolyard_to_playgrounds'),
        ref('int_buffers__nys_parks_properties'),
        ref('int_buffers__nysdec_state_facility_permits'),
        ref('int_buffers__nysdec_title_v_facility_permits'),
        ref('int_buffers__pops'),
        ref('int_buffers__us_parks_properties'),
        ref('int_buffers__waterfront_access_pow'),
        ref('int_buffers__waterfront_access_wpaa'),
        ref('int_buffers__natural_resource_shadows'),
        ref('stg__panynj_airports'),
        ref('stg__dcp_beaches'),
        ref('stg__dcp_wrp_rec'),
        ref('stg__dcp_wrp_snwa'),
        ref('stg__dpr_forever_wild'),
        ref('stg__nysdec_freshwater_wetlands_checkzones'),
        ref('stg__nysdec_freshwater_wetlands'),
        ref('stg__nysdec_natural_heritage_communities'),
        ref('stg__nysdec_priority_estuaries'),
        ref('stg__nysdec_priority_lakes'),
        ref('stg__nysdec_priority_streams'),
        ref('stg__nysdec_tidal_wetlands'),
        ref('stg__usfws_nyc_wetlands'),
        ref('int_buffers__nysshpo_historic_buildings'),
        ref('stg__nysshpo_historic_building_districts'),
        ref('int_buffers__nysparks_historicplaces'),
        ref('stg__lpc_historic_district_areas'),
        ref('stg__lpc_scenic_landmarks'),
        ref('int_buffers__lpc_landmarks'),
        ref('stg__nysshpo_archaeological_buffer_areas')
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
    COALESCE(buffer, raw_geom) AS buffer
FROM all_buffers
