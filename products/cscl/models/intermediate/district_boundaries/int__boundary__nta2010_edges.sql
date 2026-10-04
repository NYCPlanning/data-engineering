{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id']}]
) }}

-- Every topology edge classified relative to each 2010 NTA that owns it. Land only.

{{ district_boundary_classify_edges('nta2010') }}
