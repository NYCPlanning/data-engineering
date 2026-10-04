{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id']}]
) }}

-- Every topology edge classified relative to each school district as published
-- (District 10 split per borough as '10-<borocode>', others whole). Water APs carrying the district are part of it, as for sd.

{{ district_boundary_classify_edges('sd_boro', include_assigned_water=true) }}
