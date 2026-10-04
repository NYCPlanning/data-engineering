{{ config(materialized='table') }}

-- int__boundary__mcwi vs production_outputs.fgdb_nymcwi.

{{ district_boundary_vs_prod(
    'int__boundary__mcwi',
    'qa__boundary__mcwi_validity',
    'fgdb_nymcwi',
    'borocode::text || p.municourt',
    'mc'
) }}
