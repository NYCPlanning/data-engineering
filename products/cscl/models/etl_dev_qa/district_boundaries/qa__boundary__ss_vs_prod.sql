{{ config(materialized='table') }}

-- int__boundary__ss vs production_outputs.fgdb_nyss.

{{ district_boundary_vs_prod('int__boundary__ss', 'qa__boundary__ss_validity', 'fgdb_nyss', 'stsendist', 'ss') }}
