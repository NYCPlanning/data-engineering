{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id']}]
) }}

-- Every topology edge classified relative to each 2020 census tract that owns it. Land only.

{{ district_boundary_classify_edges('boroct2020') }}
