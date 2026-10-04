{{ config(materialized='table') }}

-- int__boundary__fd vs production_outputs.fgdb_nyfd.

{{ district_boundary_vs_prod(
    'int__boundary__fd',
    'qa__boundary__fd_validity',
    'fgdb_nyfd',
    'firediv::text',
    'fire_division'
) }}
