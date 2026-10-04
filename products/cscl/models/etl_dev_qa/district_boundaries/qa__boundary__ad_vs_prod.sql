{{ config(materialized='table') }}

-- int__boundary__ad vs production_outputs.fgdb_nyad.

{{ district_boundary_vs_prod('int__boundary__ad', 'qa__boundary__ad_validity', 'fgdb_nyad', 'assemdist', 'ad') }}
