{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id']}]
) }}

-- Every topology edge classified relative to each state senate district, water included: every
-- water AP carrying the district's id is part of it (the *wi layers cover water too).

{{ district_boundary_classify_edges('ss', include_assigned_water=true) }}
