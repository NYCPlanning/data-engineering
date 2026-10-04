{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id']}]
) }}

-- Every topology edge classified relative to each health center district that owns it. Land only.

{{ district_boundary_classify_edges('health_center') }}
