{{ config(materialized='table') }}

-- int__boundary__cgwi vs production_outputs.fgdb_nycgwi.

{{ district_boundary_vs_prod('int__boundary__cgwi', 'qa__boundary__cgwi_validity', 'fgdb_nycgwi', 'congdist', 'cg') }}
