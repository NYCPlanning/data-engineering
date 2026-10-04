{{ config(materialized='table') }}

-- int__boundary__nta2010 vs production_outputs.fgdb_nynta2010.

{{ district_boundary_vs_prod(
    'int__boundary__nta2010',
    'qa__boundary__nta2010_validity',
    'fgdb_nynta2010',
    "ntacode",
    'nta2010'
) }}
