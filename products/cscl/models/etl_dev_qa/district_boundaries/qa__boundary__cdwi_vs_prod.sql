{{ config(materialized='table') }}

-- int__boundary__cdwi vs production_outputs.fgdb_nycdwi.

{{ district_boundary_vs_prod('int__boundary__cdwi', 'qa__boundary__cdwi_validity', 'fgdb_nycdwi', 'borocd', 'cd') }}
