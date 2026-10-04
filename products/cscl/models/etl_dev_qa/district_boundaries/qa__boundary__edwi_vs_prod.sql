{{ config(materialized='table') }}

-- int__boundary__edwi vs production_outputs.fgdb_nyedwi.

{{ district_boundary_vs_prod('int__boundary__edwi', 'qa__boundary__edwi_validity', 'fgdb_nyedwi', 'electdist', 'ed') }}
