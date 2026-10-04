{{ config(materialized='table') }}

-- int__boundary__ccwi vs production_outputs.fgdb_nyccwi.

{{ district_boundary_vs_prod('int__boundary__ccwi', 'qa__boundary__ccwi_validity', 'fgdb_nyccwi', 'coundist', 'cc') }}
