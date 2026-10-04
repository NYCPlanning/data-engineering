{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id']}]
) }}

-- Every topology edge classified relative to each fire division that owns it. Water APs assigned to a fire division are part of it, as in the published layer.

{{ district_boundary_classify_edges('fire_division', include_assigned_water=true) }}
