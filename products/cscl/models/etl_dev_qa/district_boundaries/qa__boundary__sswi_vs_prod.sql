{{ config(materialized='table') }}

-- int__boundary__sswi vs production_outputs.fgdb_nysswi.

{{ district_boundary_vs_prod('int__boundary__sswi', 'qa__boundary__sswi_validity', 'fgdb_nysswi', 'stsendist', 'ss') }}
