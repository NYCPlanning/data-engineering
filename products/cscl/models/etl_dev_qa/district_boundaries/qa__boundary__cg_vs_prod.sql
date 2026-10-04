{{ config(materialized='table') }}

-- int__boundary__cg vs production_outputs.fgdb_nycg.

{{ district_boundary_vs_prod('int__boundary__cg', 'qa__boundary__cg_validity', 'fgdb_nycg', 'congdist', 'cg') }}
