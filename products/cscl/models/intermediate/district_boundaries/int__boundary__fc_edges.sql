{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id']}]
) }}

-- Every topology edge classified relative to each fire company that owns it. Water APs assigned to a fire company are part of it, as in the published layer.

{{ district_boundary_classify_edges('fire_company', include_assigned_water=true) }}
