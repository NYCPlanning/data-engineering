{{ config(materialized='table') }}

-- int__boundary__cc vs production_outputs.fgdb_nycc.

{{ district_boundary_vs_prod('int__boundary__cc', 'qa__boundary__cc_validity', 'fgdb_nycc', 'coundist', 'cc') }}
