{{ config(materialized='table') }}

-- int__boundary__bb vs production_outputs.fgdb_nybb.

{{ district_boundary_vs_prod('int__boundary__bb', 'qa__boundary__bb_validity', 'fgdb_nybb', 'borocode', 'bb') }}
