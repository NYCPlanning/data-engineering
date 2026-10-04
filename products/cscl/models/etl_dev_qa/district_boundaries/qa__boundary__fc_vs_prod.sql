{{ config(materialized='table') }}

-- int__boundary__fc vs production_outputs.fgdb_nyfc.

{{ district_boundary_vs_prod(
    'int__boundary__fc',
    'qa__boundary__fc_validity',
    'fgdb_nyfc',
    "firecotype || ' ' || fireconum::text",
    'fire_company'
) }}
