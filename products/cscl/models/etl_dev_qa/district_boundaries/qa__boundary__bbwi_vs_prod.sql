{{ config(materialized='table') }}

-- int__boundary__bbwi vs production_outputs.fgdb_nybbwi.

{{ district_boundary_vs_prod('int__boundary__bbwi', 'qa__boundary__bbwi_validity', 'fgdb_nybbwi', 'borocode', 'bb') }}
