{{ config(materialized='table') }}

-- int__boundary__ed vs production_outputs.fgdb_nyed.

{{ district_boundary_vs_prod('int__boundary__ed', 'qa__boundary__ed_validity', 'fgdb_nyed', 'electdist', 'ed') }}
