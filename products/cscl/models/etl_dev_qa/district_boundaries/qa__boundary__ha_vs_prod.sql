{{ config(materialized='table') }}

-- int__boundary__ha vs production_outputs.fgdb_nyha.

{{ district_boundary_vs_prod(
    'int__boundary__ha',
    'qa__boundary__ha_validity',
    'fgdb_nyha',
    "borocode::text || lpad(healtharea::text, 4, '0')",
    'health_area'
) }}
