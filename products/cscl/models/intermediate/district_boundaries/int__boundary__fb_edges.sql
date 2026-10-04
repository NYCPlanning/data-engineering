{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id']}]
) }}

-- Every topology edge classified relative to each fire battalion that owns it. Water APs assigned to a fire battalion are part of it, as in the published layer.

{{ district_boundary_classify_edges('fire_battalion', include_assigned_water=true) }}
