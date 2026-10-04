{{ config(materialized='table') }}

-- int__boundary__adwi vs production_outputs.fgdb_nyadwi.

{{ district_boundary_vs_prod('int__boundary__adwi', 'qa__boundary__adwi_validity', 'fgdb_nyadwi', 'assemdist', 'ad') }}
