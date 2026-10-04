{{ config(materialized='table') }}

-- int__boundary__cd vs production_outputs.fgdb_nycd.

{{ district_boundary_vs_prod('int__boundary__cd', 'qa__boundary__cd_validity', 'fgdb_nycd', 'borocd', 'cd') }}
