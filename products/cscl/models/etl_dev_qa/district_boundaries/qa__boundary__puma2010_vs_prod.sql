{{ config(materialized='table') }}

-- int__boundary__puma2010 vs production_outputs.fgdb_nypuma2010.

{{ district_boundary_vs_prod(
    'int__boundary__puma2010',
    'qa__boundary__puma2010_validity',
    'fgdb_nypuma2010',
    'puma',
    'puma'
) }}
