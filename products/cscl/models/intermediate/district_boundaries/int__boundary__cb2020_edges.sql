{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id']}]
) }}

-- Every topology edge classified relative to each 2020 census block that owns it. Land only.

{{ district_boundary_classify_edges('cb2020') }}
