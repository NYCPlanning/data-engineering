{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id']}]
) }}

-- Every topology edge classified relative to each school district that owns it. Water APs
-- carrying this district's id are part of it, as in the published layer.

{{ district_boundary_classify_edges('sd', include_assigned_water=true) }}
