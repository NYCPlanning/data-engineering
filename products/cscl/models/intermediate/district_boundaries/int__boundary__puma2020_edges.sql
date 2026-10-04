{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id']}]
) }}

-- Every topology edge classified relative to each 2020 PUMA that owns it. Land only.

{{ district_boundary_classify_edges('puma2020') }}
