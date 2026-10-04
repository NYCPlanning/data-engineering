{{ config(materialized='table') }}

-- int__boundary__mc vs production_outputs.fgdb_nymc.

{{ district_boundary_vs_prod(
    'int__boundary__mc',
    'qa__boundary__mc_validity',
    'fgdb_nymc',
    'borocode::text || p.municourt',
    'mc'
) }}
