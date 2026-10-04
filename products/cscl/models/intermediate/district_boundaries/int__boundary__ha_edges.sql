{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id']}]
) }}

-- Every topology edge classified relative to each health area that owns it. Land only.

{{ district_boundary_classify_edges('health_area') }}
